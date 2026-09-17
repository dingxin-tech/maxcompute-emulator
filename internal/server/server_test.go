package server

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
	"reflect"
)

func fixture(t *testing.T, c Config) (*Server, *httptest.Server) {
	t.Helper()
	e, err := engine.Open("", 10000)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	values := make([]string, 2048)
	for i := range values {
		values[i] = fmt.Sprintf("(%d,'%s')", i, strings.Repeat("value", 20))
	}
	_, err = e.Execute(context.Background(), "p", "default", "create table t(id bigint,s string);insert into t values "+strings.Join(values, ","))
	if err != nil {
		t.Fatal(err)
	}
	s := New(e, c)
	h := httptest.NewServer(s)
	t.Cleanup(h.Close)
	return s, h
}
func request(t *testing.T, h *httptest.Server, method, path, encoding string) (int, http.Header, []byte) {
	t.Helper()
	r, _ := http.NewRequest(method, h.URL+path, nil)
	if encoding == "" {
		encoding = "identity"
	}
	if encoding != "" {
		r.Header.Set("Accept-Encoding", encoding)
	}
	resp, e := h.Client().Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(resp.Body)
	if e != nil {
		t.Fatal(e)
	}
	return resp.StatusCode, resp.Header, b
}
func sessionID(t *testing.T, h *httptest.Server) string {
	t.Helper()
	code, _, b := request(t, h, "POST", "/projects/p/tables/t?downloads&asyncmode=true&quotaName=q", "")
	if code != 200 {
		t.Fatalf("%d %s", code, b)
	}
	var m map[string]any
	if e := json.Unmarshal(b, &m); e != nil {
		t.Fatal(e)
	}
	if m["RecordCount"] != float64(2048) || m["Status"] != "normal" || m["QuotaName"] != "q" || m["Owner"] == nil || m["Initiated"] == nil {
		t.Fatal(m)
	}
	return m["DownloadID"].(string)
}
func unchunk(t *testing.T, b []byte) []byte {
	t.Helper()
	if len(b) < 8 {
		t.Fatal("short Arrow frame")
	}
	size := int(binary.BigEndian.Uint32(b))
	b = b[4:]
	if size != 65536 {
		t.Fatal(size)
	}
	out := []byte{}
	tab := crc32.MakeTable(crc32.Castagnoli)
	for len(b) > size+4 {
		chunk := b[:size]
		sum := binary.BigEndian.Uint32(b[size:])
		if crc32.Checksum(chunk, tab) != sum {
			t.Fatal("chunk checksum")
		}
		out = append(out, chunk...)
		b = b[size+4:]
	}
	out = append(out, b[:len(b)-4]...)
	if crc32.Checksum(out, tab) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		t.Fatal("global checksum")
	}
	mr := ipc.NewMessageReader(bytes.NewReader(out))
	defer mr.Release()
	msg, e := mr.Message()
	if e != nil {
		t.Fatal(e)
	}
	if msg.Type() != ipc.MessageRecordBatch {
		t.Fatalf("expected schema-less batch, got %s", msg.Type())
	}
	if _, e = mr.Message(); !errors.Is(e, io.EOF) {
		t.Fatalf("one batch required: %v", e)
	}
	return out
}
func TestArrowCompressionChunkCRCAndRawLimit(t *testing.T) {
	_, h := fixture(t, Config{})
	id := sessionID(t, h)
	path := "/projects/p/tables/t?downloadid=" + id + "&data&arrow&rowrange=(0,2048)"
	code, header, raw := request(t, h, "GET", path, "")
	if code != 200 {
		t.Fatalf("%d %s", code, raw)
	}
	if len(raw) < 65536 {
		t.Fatal("fixture must span chunks")
	}
	unchunk(t, raw)
	for _, alg := range []string{"zstd", "lz4_frame", "x-lz4-frame"} {
		code, head, b := request(t, h, "GET", path, alg)
		if code != 200 || head.Get("Content-Encoding") != alg {
			t.Fatalf("%d %v", code, head)
		}
		cols := []engine.Column{{Name: "id", Type: "bigint", Parsed: engine.Type{Name: "bigint"}}, {Name: "s", Type: "string", Parsed: engine.Type{Name: "string"}}}
		got, err := wire.DecodeArrow(b, cols, true)
		want, wantErr := wire.DecodeArrow(raw, cols, true)
		if err != nil || wantErr != nil || !reflect.DeepEqual(got.Rows, want.Rows) {
			t.Fatalf("%s IPC decode: %v / %v rows=%d", alg, err, wantErr, len(got.Rows))
		}

	}
	code, head, small := request(t, h, "GET", path+"&raw_size=2000", "")
	if code != 200 || len(small) >= len(raw) || head.Get("Last-Modified") != header.Get("Last-Modified") {
		t.Fatalf("%d size=%d", code, len(small))
	}
	unchunk(t, small)
	code, _, b := request(t, h, "GET", path, "deflate")
	if code != 200 {
		t.Fatalf("deflate status %d", code)
	}
	zr, err := zlib.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	got, err := io.ReadAll(zr)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatal("deflate payload mismatch", err)
	}
}
func TestSessionBindingErrorsAndCompletion(t *testing.T) {
	_, h := fixture(t, Config{MaxSessions: 1})
	id := sessionID(t, h)
	base := "/projects/p/tables/t?downloadid=" + id
	for _, path := range []string{"/projects/other/tables/t?downloadid=" + id, base + "&curr_schema=other", base + "&partition=ds=other"} {
		code, _, _ := request(t, h, "GET", path, "")
		if code != 404 {
			t.Fatal(path, code)
		}
	}
	for _, suffix := range []string{"&rowrange=(-1,2)", "&rowrange=(99999,1)", "&rowrange=(0,9999999999999999999999999)", "&rowrange=(0,2)&columns=missing", "&rowrange=(0,2)&columns=id,id", "&rowrange=(0,2)&raw_size=-1"} {
		code, _, b := request(t, h, "GET", base+"&data"+suffix, "")
		if code != 400 {
			t.Fatalf("%s: %d %s", suffix, code, b)
		}
	}
	code, _, _ := request(t, h, "POST", "/projects/p/tables/t?downloads", "")
	if code != 429 {
		t.Fatal(code)
	}
	code, _, _ = request(t, h, "POST", base, "")
	if code != 200 {
		t.Fatal(code)
	}
	code, _, _ = request(t, h, "GET", base, "")
	if code != 404 {
		t.Fatal(code)
	}
	sessionID(t, h)
}
func TestSessionExpiry(t *testing.T) {
	s, h := fixture(t, Config{})
	id := sessionID(t, h)
	s.mu.Lock()
	s.sessions[id].Created = time.Now().Add(-time.Hour)
	s.mu.Unlock()
	code, _, _ := request(t, h, "GET", "/projects/p/tables/t?downloadid="+id, "")
	if code != 404 {
		t.Fatal(code)
	}
}

func TestMissingProjectAndPartitionContracts(t *testing.T) {
	s, h := fixture(t, Config{})
	for _, path := range []string{"/projects/missing", "/projects/missing/tunnel", "/projects/missing/tables/t", "/projects/missing/tables/t?downloads", "/projects/missing/tables/t?downloadid=missing", "/projects/missing/instances"} {
		method := "GET"
		if strings.Contains(path, "downloads") || strings.HasSuffix(path, "instances") {
			method = "POST"
		}
		code, head, b := request(t, h, method, path, "")
		if code != 404 || !strings.Contains(string(b), "NoSuchProject") || head.Get("x-odps-request-id") == "" {
			t.Errorf("%s: %d %s", path, code, b)
		}
	}
	_, err := s.Engine.Execute(context.Background(), "p", "default", "create table pt(id bigint) partitioned by (ds string);alter table pt add partition(ds='empty')")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		part   string
		status int
		code   string
	}{{"ds=missing", 404, "NoSuchPartition"}, {"other=x", 400, "InvalidPartitionSpec"}, {"", 400, "InvalidPartitionSpec"}, {"ds=empty", 200, "RecordCount"}} {
		code, head, b := request(t, h, "POST", "/projects/p/tables/pt?downloads&partition="+tc.part, "")
		if code != tc.status || !strings.Contains(string(b), tc.code) || head.Get("x-odps-request-id") == "" {
			t.Errorf("%s: %d %s", tc.part, code, b)
		}
	}
	if len(s.sessions) != 1 {
		t.Fatalf("failed requests allocated sessions: %d", len(s.sessions))
	}
	code, _, b := request(t, h, "POST", "/projects/p/tables/missing?downloads", "")
	if code != 404 || !strings.Contains(string(b), "NoSuchTable") {
		t.Fatalf("%d %s", code, b)
	}
}

func TestArrowIPCRangesAndMultipleBatches(t *testing.T) {
	s, h := fixture(t, Config{})
	_, err := s.Engine.Execute(context.Background(), "p", "default", "create table many(id bigint,s string);insert into many select i, 'v' from range(9000) t(i);create table empty(id bigint,s string)")
	if err != nil {
		t.Fatal(err)
	}
	cols := []engine.Column{{Name: "id", Type: "bigint", Parsed: engine.Type{Name: "bigint"}}, {Name: "s", Type: "string", Parsed: engine.Type{Name: "string"}}}
	for _, tc := range []struct {
		table                 string
		start, count, batches int
	}{{"t", 0, 8, 1}, {"many", 0, 9000, 3}, {"empty", 0, 0, 0}, {"many", 3, 0, 0}} {
		code, _, b := request(t, h, "POST", "/projects/p/tables/"+tc.table+"?downloads", "")
		if code != 200 {
			t.Fatalf("%d %s", code, b)
		}
		var sess map[string]any
		if err := json.Unmarshal(b, &sess); err != nil {
			t.Fatal(err)
		}
		for _, alg := range []string{"identity", "zstd", "lz4_frame"} {
			path := fmt.Sprintf("/projects/p/tables/%s?downloadid=%s&data&arrow&rowrange=(%d,%d)", tc.table, sess["DownloadID"], tc.start, tc.count)
			code, _, b = request(t, h, "GET", path, alg)
			if code != 200 {
				t.Fatalf("%s: %d %s", alg, code, b)
			}
			got, err := wire.DecodeArrow(b, cols, true)
			if err != nil || len(got.Rows) != tc.count {
				t.Fatalf("%s %s rows=%d: %v", tc.table, alg, len(got.Rows), err)
			}
			for i, row := range got.Rows {
				if row[0] != int64(tc.start+i) {
					t.Fatalf("out of order or duplicate: %v", row)
				}
			}
			raw, err := wire.Unchunk(b)
			if err != nil {
				t.Fatal(err)
			}
			reader := ipc.NewMessageReader(bytes.NewReader(raw))
			batches := 0
			for {
				msg, err := reader.Message()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if msg.Type() != ipc.MessageRecordBatch {
					t.Fatal(msg.Type())
				}
				batches++
			}
			reader.Release()
			if batches != tc.batches {
				t.Fatalf("%s %s batches=%d", tc.table, alg, batches)
			}
		}
	}
}

func TestConfiguredEmptyProjectAndStorageProjectErrors(t *testing.T) {
	e, err := engine.Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	s := New(e, Config{Project: "custom"})
	h := httptest.NewServer(s)
	defer h.Close()
	for _, p := range []string{"/projects/custom", "/projects/custom/tunnel", "/projects/custom/tables"} {
		code, _, b := request(t, h, "GET", p, "")
		if code != 200 {
			t.Fatalf("%s: %d %s", p, code, b)
		}
	}
	for _, target := range []string{"projects.missing.schemas.default.tables.t", "projects.missing.instances.i"} {
		code, head, b := request(t, h, "POST", "/api/storage/v2?Action=TableCreateReadSession&Target="+target, "")
		if code != 404 || !strings.Contains(string(b), "NoSuchProject") || head.Get("x-odps-request-id") == "" {
			t.Fatalf("%d %s", code, b)
		}
	}
	if len(s.sessions) != 0 || len(s.storage) != 0 {
		t.Fatal("missing project created session")
	}
}
