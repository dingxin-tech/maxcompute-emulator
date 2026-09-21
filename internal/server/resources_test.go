package server

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

func md5hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func resourceRequest(t *testing.T, s *Server, method, path, name, kind string, headers map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if name != "" {
		r.Header.Set("x-odps-resource-name", name)
	}
	if kind != "" {
		r.Header.Set("x-odps-resource-type", kind)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

func (s *Server) do(t *testing.T, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader("")))
	return w
}

func TestCapabilitiesDeclareMetadataPlane(t *testing.T) {
	s := New(mustEngine(t), Config{Project: "p"})
	w := s.do(t, "GET", "/capabilities")
	if w.Code != 200 {
		t.Fatalf("capabilities: %d %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, want := range []string{`"resources":["file","jar","py","archive","table-metadata"]`, `"java-udf-metadata"`, `"sql-function-metadata"`, `"embedded-function-metadata"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("capabilities missing %s: %s", want, body)
		}
	}
	// Chunked part upload is supported, so it must not be advertised as a gap.
	if strings.Contains(body, "chunked-resource-upload") {
		t.Fatalf("stale unsupported entry: %s", body)
	}
	for _, want := range []string{"volume-resources", "sql-udf-execution"} {
		if !strings.Contains(body, want) {
			t.Fatalf("capabilities missing declared gap %s: %s", want, body)
		}
	}
}

func mustEngine(t *testing.T) *engine.Engine {
	t.Helper()
	e, err := engine.Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func TestResourceRESTContract(t *testing.T) {
	e, err := engine.Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	s := New(e, Config{Project: "p"})

	w := resourceRequest(t, s, "POST", "/projects/p/resources", "WordCount.py", "py", map[string]string{"x-odps-comment": "udf dep"}, "print(1)")
	if w.Code != 201 || !strings.Contains(w.Header().Get("Location"), "/resources/WordCount.py") {
		t.Fatalf("create: %d %s %s", w.Code, w.Header().Get("Location"), w.Body.String())
	}
	w = resourceRequest(t, s, "POST", "/projects/p/resources", "wordcount.py", "py", nil, "duplicate")
	if w.Code != 400 || !strings.Contains(w.Body.String(), "<Code>ResourceAlreadyExists</Code>") {
		t.Fatalf("duplicate create: %d %s", w.Code, w.Body.String())
	}
	w = resourceRequest(t, s, "GET", "/projects/p/resources", "", "", nil, "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "<ResourceType>PY</ResourceType>") || !strings.Contains(w.Body.String(), "<ResourceSize>8</ResourceSize>") || !strings.Contains(w.Body.String(), "<Comment>udf dep</Comment>") {
		t.Fatalf("list: %d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "<Marker>x</Marker>") {
		t.Fatalf("exhausted listing must clear the marker: %s", w.Body.String())
	}
	w = resourceRequest(t, s, "GET", "/projects/p/resources/WORDCOUNT.PY?meta", "", "", nil, "")
	if w.Code != 200 || w.Header().Get("x-odps-resource-type") != "PY" || w.Header().Get("x-odps-resource-size") != "8" || w.Header().Get("x-odps-comment") != "udf dep" || w.Header().Get("x-odps-creation-time") == "" || w.Header().Get("Last-Modified") == "" {
		t.Fatalf("meta: %d %#v", w.Code, w.Header())
	}
	w = resourceRequest(t, s, "GET", "/projects/p/resources/wordcount.py", "", "", nil, "")
	if w.Code != 200 || w.Body.String() != "print(1)" || !strings.Contains(w.Header().Get("Content-Disposition"), "WordCount.py") || w.Header().Get("x-odps-resource-has-remaining") != "false" {
		t.Fatalf("download: %d %q %#v", w.Code, w.Body.String(), w.Header())
	}
	w = resourceRequest(t, s, "GET", "/projects/p/resources/wordcount.py?rOffset=3&rSize=2", "", "", nil, "")
	if w.Code != 200 || w.Body.String() != "nt" || w.Header().Get("x-odps-resource-has-remaining") != "true" {
		t.Fatalf("ranged download: %d %q %s", w.Code, w.Body.String(), w.Header().Get("x-odps-resource-has-remaining"))
	}
	w = resourceRequest(t, s, "PUT", "/projects/p/resources/wordcount.py", "", "py", nil, "print(2)")
	if w.Code != 200 {
		t.Fatalf("update: %d %s", w.Code, w.Body.String())
	}
	w = resourceRequest(t, s, "GET", "/projects/p/resources/wordcount.py", "", "", nil, "")
	if w.Body.String() != "print(2)" {
		t.Fatalf("updated content: %q", w.Body.String())
	}
	w = resourceRequest(t, s, "POST", "/projects/p/resources/wordcount.py", "", "jar", nil, "blocked-by-path-mismatch")
	if w.Code != 400 || !strings.Contains(w.Body.String(), "InvalidResourceType") {
		t.Fatalf("type change on update: %d %s", w.Code, w.Body.String())
	}
	resourceRequest(t, s, "POST", "/projects/p/resources", "part.bin", "file", nil, "x")
	w = resourceRequest(t, s, "GET", "/projects/p/resources?name=part&type=FILE", "", "", nil, "")
	if !strings.Contains(w.Body.String(), "<Name>part.bin</Name>") {
		t.Fatalf("type filter: %d %s", w.Code, w.Body.String())
	}
	w = resourceRequest(t, s, "GET", "/projects/p/resources?name=part&type=PY", "", "", nil, "")
	if strings.Contains(w.Body.String(), "<Name>part.bin</Name>") {
		t.Fatalf("type filter leaked: %s", w.Body.String())
	}
	for _, tc := range []struct{ method, path, want string }{
		{"GET", "/projects/p/resources/missing.py", "NoSuchObject"},
		{"GET", "/projects/p/resources/missing.py?meta", "NoSuchObject"},
		{"DELETE", "/projects/p/resources/missing.py", "NoSuchObject"},
		{"PUT", "/projects/p/resources/missing.py", "NoSuchObject"},
	} {
		w = resourceRequest(t, s, tc.method, tc.path, "missing.py", "file", nil, "x")
		if w.Code != 404 || !strings.Contains(w.Body.String(), "<Code>"+tc.want+"</Code>") {
			t.Fatalf("%s %s: %d %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
	w = resourceRequest(t, s, "POST", "/projects/p/resources", "vol", "file", map[string]string{"x-odps-copy-file-source": "vol/path"}, "x")
	if w.Code != 400 || !strings.Contains(w.Body.String(), "UnsupportedOperation") {
		t.Fatalf("volume resource: %d %s", w.Code, w.Body.String())
	}
	// The Java SDK always chunks file uploads: part resources first, then a
	// merge that names the final resource.
	partA, partB := "chunked.py.part.tmp.000000", "chunked.py.part.tmp.000001"
	temp := map[string]string{"x-odps-resource-istemp": "true"}
	w = resourceRequest(t, s, "POST", "/projects/p/resources?rIsPart=true", partA, "file", nil, "print(")
	if w.Code != 400 || !strings.Contains(w.Body.String(), "istemp") {
		t.Fatalf("part without the temp flag: %d %s", w.Code, w.Body.String())
	}
	for _, part := range []struct{ name, body string }{{partA, "print("}, {partB, "1)"}} {
		w = resourceRequest(t, s, "POST", "/projects/p/resources?rIsPart=true", part.name, "file", temp, part.body)
		if w.Code != 200 {
			t.Fatalf("part %s: %d %s", part.name, w.Code, w.Body.String())
		}
	}
	// SDK retries re-POST the same deterministic part name; the payload must not grow.
	w = resourceRequest(t, s, "POST", "/projects/p/resources?rIsPart=true", partA, "file", temp, "print(")
	if w.Code != 200 {
		t.Fatalf("part retry: %d %s", w.Code, w.Body.String())
	}
	w = resourceRequest(t, s, "POST", "/projects/p/resources?rOpMerge=true", "chunked.py", "py",
		map[string]string{"x-odps-resource-merge-total-bytes": "8"}, md5hex("print(1)")+"|"+partA+","+partB)
	if w.Code != 201 {
		t.Fatalf("merge: %d %s", w.Code, w.Body.String())
	}
	w = resourceRequest(t, s, "GET", "/projects/p/resources/chunked.py", "", "", nil, "")
	if w.Body.String() != "print(1)" {
		t.Fatalf("merged content: %q", w.Body.String())
	}
	w = resourceRequest(t, s, "GET", "/projects/p/resources/chunked.py?meta", "", "", nil, "")
	if w.Header().Get("Content-MD5") != md5hex("print(1)") {
		t.Fatalf("merged meta MD5: %#v", w.Header())
	}
	w = resourceRequest(t, s, "GET", "/projects/p/resources/"+partA, "", "", nil, "")
	if w.Code != 404 {
		t.Fatalf("parts must be cleaned after merge: %d %s", w.Code, w.Body.String())
	}
	// A mismatching digest or byte count must not publish a partial payload.
	for _, body := range []string{md5hex("nope") + "|" + partB, "0123456789abcdef0123456789abcdef|" + partB} {
		resourceRequest(t, s, "POST", "/projects/p/resources?rIsPart=true", partB, "file", temp, "x")
		w = resourceRequest(t, s, "POST", "/projects/p/resources?rOpMerge=true", "bad.py", "py", nil, body)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "InvalidParameter") {
			t.Fatalf("bad merge %q: %d %s", body, w.Code, w.Body.String())
		}
		if w = resourceRequest(t, s, "GET", "/projects/p/resources/bad.py", "", "", nil, ""); w.Code != 404 {
			t.Fatalf("failed merge must not publish: %d %s", w.Code, w.Body.String())
		}
	}
	resourceRequest(t, s, "POST", "/projects/p/resources?rIsPart=true", partB, "file", temp, "x")
	w = resourceRequest(t, s, "POST", "/projects/p/resources?rOpMerge=true", "bad.py", "py", map[string]string{"x-odps-resource-merge-total-bytes": "99"}, md5hex("x")+"|"+partB)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "merge-total-bytes") {
		t.Fatalf("declared size: %d %s", w.Code, w.Body.String())
	}
	w = resourceRequest(t, s, "POST", "/projects/p/resources?rOpMerge=true", "bad.py", "py", nil, md5hex("x")+"|absent_part")
	if w.Code != 404 || !strings.Contains(w.Body.String(), "NoSuchObject") {
		t.Fatalf("missing part: %d %s", w.Code, w.Body.String())
	}
	// Chunked overwrite replaces an existing resource through PUT on the item.
	w = resourceRequest(t, s, "POST", "/projects/p/resources?rIsPart=true", partA, "file", temp, "print(2)")
	w = resourceRequest(t, s, "PUT", "/projects/p/resources/chunked.py?rOpMerge=true", "chunked.py", "py", nil, md5hex("print(2)")+"|"+partA)
	if w.Code != 200 {
		t.Fatalf("chunked overwrite: %d %s", w.Code, w.Body.String())
	}
	if w = resourceRequest(t, s, "GET", "/projects/p/resources/chunked.py", "", "", nil, ""); w.Body.String() != "print(2)" {
		t.Fatalf("chunked overwrite content: %q", w.Body.String())
	}
	// A TABLE resource keeps metadata only and validates the referenced table.
	if _, err = e.Execute(context.Background(), "p", "default", "create table src(id bigint);"); err != nil {
		t.Fatal(err)
	}
	w = resourceRequest(t, s, "POST", "/projects/p/resources", "snap", "table", map[string]string{"x-odps-copy-table-source": "src"}, "")
	if w.Code != 201 {
		t.Fatalf("table resource: %d %s", w.Code, w.Body.String())
	}
	w = resourceRequest(t, s, "GET", "/projects/p/resources/snap", "", "", nil, "")
	if w.Code != 400 || !strings.Contains(w.Body.String(), "UnsupportedOperation") {
		t.Fatalf("table resource download: %d %s", w.Code, w.Body.String())
	}
	w = resourceRequest(t, s, "GET", "/projects/p/resources/snap?meta", "", "", nil, "")
	if w.Header().Get("x-odps-copy-table-source") != "src" || w.Header().Get("x-odps-resource-type") != "TABLE" {
		t.Fatalf("table resource meta: %#v", w.Header())
	}
	w = resourceRequest(t, s, "GET", "/projects/p/resources?name=snap", "", "", nil, "")
	if !strings.Contains(w.Body.String(), "<TableName>src</TableName>") {
		t.Fatalf("table resource listing: %s", w.Body.String())
	}
}

func TestFunctionRESTContract(t *testing.T) {
	e, err := engine.Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	s := New(e, Config{Project: "p"})
	if w := resourceRequest(t, s, "POST", "/projects/p/resources", "wc.py", "py", nil, "print(1)"); w.Code != 201 {
		t.Fatalf("resource: %d %s", w.Code, w.Body.String())
	}
	body := "<Function><Alias>word_count</Alias><ClassType>com.example.WC</ClassType><Resources><ResourceName>wc.py</ResourceName><ResourceName>wc.py</ResourceName></Resources></Function>"
	w := resourceRequest(t, s, "POST", "/projects/p/registration/functions", "", "", map[string]string{"Content-Type": "application/xml"}, body)
	if w.Code != 201 || !strings.Contains(w.Header().Get("Location"), "/registration/functions/word_count") {
		t.Fatalf("create: %d %s %s", w.Code, w.Header().Get("Location"), w.Body.String())
	}
	w = s.do(t, "GET", "/projects/p/registration/functions/WORD_COUNT")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "<Alias>word_count</Alias>") || strings.Count(w.Body.String(), "<ResourceName>") != 1 || !strings.Contains(w.Body.String(), "<ClassType>com.example.WC</ClassType>") {
		t.Fatalf("get: %d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "<CreationTime>") || !strings.Contains(w.Body.String(), "<Owner>emulator</Owner>") {
		t.Fatalf("get fields: %s", w.Body.String())
	}
	w = resourceRequest(t, s, "POST", "/projects/p/registration/functions", "", "", map[string]string{"Content-Type": "application/xml"}, "<Function><Alias>broken</Alias><ClassType>com.example.X</ClassType><Resources><ResourceName>nope.py</ResourceName></Resources></Function>")
	if w.Code != 400 || !strings.Contains(w.Body.String(), "unavailable resource nope.py") {
		t.Fatalf("missing dependency: %d %s", w.Code, w.Body.String())
	}
	w = resourceRequest(t, s, "POST", "/projects/p/registration/functions", "", "", map[string]string{"Content-Type": "application/xml"}, "<Function><Name>legacy_alias</Name><ClassType>com.example.L</ClassType><Resources><ResourceName>wc.py</ResourceName></Resources></Function>")
	if w.Code != 201 {
		t.Fatalf("Name element accepted: %d %s", w.Code, w.Body.String())
	}
	w = resourceRequest(t, s, "POST", "/projects/p/registration/functions", "", "", map[string]string{"Content-Type": "application/xml"}, "<Function><Alias>sql_sum</Alias><IsSqlFunction>true</IsSqlFunction><SqlDefinitionText>select 1</SqlDefinitionText></Function>")
	if w.Code != 201 {
		t.Fatalf("sql function: %d %s", w.Code, w.Body.String())
	}
	w = s.do(t, "GET", "/projects/p/registration/functions/sql_sum")
	if !strings.Contains(w.Body.String(), "<IsSqlFunction>true</IsSqlFunction>") || strings.Contains(w.Body.String(), "<ClassType>") {
		t.Fatalf("sql function body: %s", w.Body.String())
	}
	w = s.do(t, "GET", "/projects/p/registration/functions?maxitems=1")
	if w.Code != 200 || strings.Count(w.Body.String(), "<Alias>") != 1 || !strings.Contains(w.Body.String(), "<Marker>legacy_alias</Marker>") {
		t.Fatalf("list page 1: %d %s", w.Code, w.Body.String())
	}
	w = s.do(t, "GET", "/projects/p/registration/functions?maxitems=1&marker=legacy_alias")
	if !strings.Contains(w.Body.String(), "<Alias>sql_sum</Alias>") || !strings.Contains(w.Body.String(), "<Marker>sql_sum</Marker>") {
		t.Fatalf("list page 2: %s", w.Body.String())
	}
	w = resourceRequest(t, s, "PUT", "/projects/p/registration/functions/word_count", "", "", map[string]string{"Content-Type": "application/xml"}, "<Function><Alias>word_count</Alias><ClassType>com.example.WC2</ClassType><Resources><ResourceName>wc.py</ResourceName></Resources></Function>")
	if w.Code != 200 {
		t.Fatalf("update: %d %s", w.Code, w.Body.String())
	}
	w = s.do(t, "GET", "/projects/p/registration/functions/word_count")
	if !strings.Contains(w.Body.String(), "com.example.WC2") {
		t.Fatalf("updated body: %s", w.Body.String())
	}
	w = resourceRequest(t, s, "POST", "/projects/p/registration/functions", "", "", map[string]string{"Content-Type": "application/xml"}, "<Function><Alias>word_count</Alias><ClassType>com.example.WC</ClassType><Resources><ResourceName>wc.py</ResourceName></Resources></Function>")
	if w.Code != 400 || !strings.Contains(w.Body.String(), "<Code>FunctionAlreadyExists</Code>") {
		t.Fatalf("duplicate: %d %s", w.Code, w.Body.String())
	}
	for _, name := range []string{"word_count", "legacy_alias", "sql_sum"} {
		if w = s.do(t, "DELETE", "/projects/p/registration/functions/"+name); w.Code != 200 {
			t.Fatalf("delete %s: %d %s", name, w.Code, w.Body.String())
		}
	}
	if w = s.do(t, "DELETE", "/projects/p/registration/functions/word_count"); w.Code != 404 {
		t.Fatalf("delete twice: %d %s", w.Code, w.Body.String())
	}
	if w = s.do(t, "DELETE", "/projects/p/registration/functions"); w.Code != 400 {
		t.Fatalf("delete collection: %d %s", w.Code, w.Body.String())
	}
	if w = s.do(t, "GET", "/projects/p/registration/functions/word_count"); w.Code != 404 || !strings.Contains(w.Body.String(), "<Code>NoSuchObject</Code>") {
		t.Fatalf("get after delete: %d %s", w.Code, w.Body.String())
	}
	if w = s.do(t, "GET", "/projects/p/resources/wc.py"); w.Code != 200 {
		t.Fatalf("resource kept after function drop: %d %s", w.Code, w.Body.String())
	}
	// SQL-side UDF registration stays an explicit gap, not a silent success.
	if _, err = e.Execute(context.Background(), "p", "default", "create function word_count as 'com.example.WC' using 'wc.py';"); err == nil || !strings.Contains(err.Error(), "UnsupportedFeature: CREATE FUNCTION") {
		t.Fatalf("SQL create function: %v", err)
	}
}
