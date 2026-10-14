package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

const acidDDL = "create table %s(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"

// createAcidTable makes an ACID table and seeds it through the SQL path, which is how
// consumers put data in before they ask for a version window.
func createAcidTable(t *testing.T, s *Server, name string, from, to int) {
	t.Helper()
	if _, err := s.Engine.Execute(context.Background(), "p", "default", fmt.Sprintf(acidDDL, name)); err != nil {
		t.Fatal(err)
	}
	insertAcidRows(t, s, name, from, to)
}

func insertAcidRows(t *testing.T, s *Server, name string, from, to int) {
	t.Helper()
	values := []string{}
	for i := from; i <= to; i++ {
		values = append(values, fmt.Sprintf("(%d,'v%d')", i, i))
	}
	if _, err := s.Engine.Execute(context.Background(), "p", "default",
		"insert into "+name+" values "+strings.Join(values, ",")); err != nil {
		t.Fatal(err)
	}
}

// openSession posts a read session and returns the status, decoded response and id.
func openSession(t *testing.T, h *httptest.Server, path, body string) (int, map[string]any, string) {
	t.Helper()
	resp, err := h.Client().Post(h.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]any{}
	if len(raw) > 0 && raw[0] == '{' {
		if err = json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
	}
	id, _ := out["SessionId"].(string)
	return resp.StatusCode, out, id
}

func eventColumns(t *testing.T, schema map[string]any) ([]string, []string) {
	t.Helper()
	system, _ := schema["SystemColumns"].([]any)
	names, types := []string{}, []string{}
	for _, c := range system {
		col, _ := c.(map[string]any)
		names = append(names, fmt.Sprint(col["Name"]))
		types = append(types, fmt.Sprint(col["Type"]))
	}
	return names, types
}

// readEvents decodes the Arrow payload of one split and returns the field order and
// the operation codes, which is everything a CDC consumer branches on.
func readEvents(t *testing.T, h *httptest.Server, table, sid string) ([]string, []int64) {
	t.Helper()
	code, _, payload := request(t, h, "GET",
		"/api/storage/v1/projects/p/schemas/default/tables/"+table+"/data?session_id="+sid+"&split_index=0", "ZSTD")
	if code != http.StatusOK {
		t.Fatalf("read %s code=%d body=%s", table, code, payload)
	}
	reader, err := ipc.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	fields, ops := []string{}, []int64{}
	for reader.Next() {
		batch := reader.RecordBatch()
		if len(fields) == 0 {
			for _, f := range batch.Schema().Fields() {
				fields = append(fields, f.Name)
			}
		}
		idx := -1
		for i, f := range batch.Schema().Fields() {
			if f.Name == engine.MetaOpColumn {
				idx = i
			}
		}
		if idx < 0 {
			t.Fatalf("event column missing from Arrow schema %v", fields)
		}
		col, ok := batch.Column(idx).(*array.Int8)
		if !ok {
			t.Fatalf("event column type %T", batch.Column(idx))
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			ops = append(ops, int64(col.Value(i)))
		}
	}
	if reader.Err() != nil {
		t.Fatal(reader.Err())
	}
	return fields, ops
}

func v1Path(table, suffix string) string {
	return "/api/storage/v1/projects/p/schemas/default/tables/" + table + suffix
}

// TestStorageIncrementalRejectsNonDelta pins the gate and its shape: the service
// refuses incremental reads on anything that is not an ACID table, with 405.
func TestStorageIncrementalRejectsNonDelta(t *testing.T) {
	_, h := fixture(t, Config{})
	cases := map[string]string{
		"v1 session type": v1Path("t", "/sessions?session_type=incremental_read"),
	}
	for label, path := range cases {
		code, body, _ := openSession(t, h, path, `{"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"append"}}`)
		if code != http.StatusMethodNotAllowed || !strings.Contains(fmt.Sprint(body), "MethodNotAllowed") {
			t.Fatalf("%s: code=%d body=%v", label, code, body)
		}
		if !strings.Contains(fmt.Sprint(body), "Only ACID table support incremental read") {
			t.Fatalf("%s: message=%v", label, body)
		}
	}
	// The v2 flag form fails the same way.
	code, body, _ := openSession(t, h, "/api/storage/v2?Action=TableCreateReadSession&Target=projects.p.schemas.default.tables.t",
		`{"IncrementalRead":true,"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"append"}}`)
	if code != http.StatusMethodNotAllowed {
		t.Fatalf("v2 non-delta: code=%d body=%v", code, body)
	}
}

// TestStorageV1IncrementalSessionShape is the consumer contract in one place: the
// window, the echoed options, the version to advance to, the system columns, and the
// Arrow field order a reader sees.
func TestStorageV1IncrementalSessionShape(t *testing.T) {
	s, h := fixture(t, Config{})
	createAcidTable(t, s, "acide", 1, 2)
	code, out, sid := openSession(t, h, v1Path("acide", "/sessions?session_type=incremental_read"),
		`{"RequiredDataColumns":["id","s"],"SplitOptions":{"SplitMode":"Size"},"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"cdc"}}`)
	if code != http.StatusOK || sid == "" {
		t.Fatalf("create: code=%d body=%v", code, out)
	}
	if out["SessionType"] != "incremental_read" {
		t.Fatalf("SessionType=%v", out["SessionType"])
	}
	if version, _ := out["LatestVersion"].(float64); version != 2 {
		t.Fatalf("LatestVersion=%v want 2", out["LatestVersion"])
	}
	if count, _ := out["RecordCount"].(float64); count != 2 {
		t.Fatalf("RecordCount=%v want 2 events", out["RecordCount"])
	}
	options, _ := out["IncrementalReadOptions"].(map[string]any)
	if options["Mode"] != "cdc" {
		t.Fatalf("echo=%v", out["IncrementalReadOptions"])
	}
	schema, _ := out["DataSchema"].(map[string]any)
	names, types := eventColumns(t, schema)
	if strings.Join(names, ",") != engine.MetaOpColumn+","+engine.MetaTSColumn {
		t.Fatalf("system columns=%v", names)
	}
	if strings.Join(types, ",") != "tinyint,timestamp" {
		t.Fatalf("system column types=%v", types)
	}
	fields, ops := readEvents(t, h, "acide", sid)
	if strings.Join(fields, ",") != "id,s,"+engine.MetaOpColumn+","+engine.MetaTSColumn {
		t.Fatalf("Arrow field order=%v", fields)
	}
	if len(ops) != 2 || ops[0] != 2 || ops[1] != 2 {
		t.Fatalf("insert events=%v", ops)
	}
	// Reloading the session reports the same window; a reader restarts from it.
	code, reload, _ := requestSession(t, h, "acide", sid)
	if code != http.StatusOK {
		t.Fatalf("reload code=%d", code)
	}
	if reload["LatestVersion"] != out["LatestVersion"] || reload["RecordCount"] != out["RecordCount"] {
		t.Fatalf("reload differs: %v vs %v", reload, out)
	}
}

func requestSession(t *testing.T, h *httptest.Server, table, sid string) (int, map[string]any, string) {
	t.Helper()
	code, _, body := request(t, h, "GET", v1Path(table, "/sessions/"+sid+"?session_type=incremental_read"), "")
	if code != http.StatusOK {
		return code, map[string]any{}, string(body)
	}
	out := map[string]any{}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("reload decode: %v %s", err, body)
	}
	id, _ := out["SessionId"].(string)
	return code, out, id
}

// TestStorageV1IncrementalWindowAdvances pins cursor behaviour: a new session starting
// at the version the consumer reached sees only newer events, and its own end version
// is where the next window must start.
func TestStorageV1IncrementalWindowAdvances(t *testing.T) {
	s, h := fixture(t, Config{})
	createAcidTable(t, s, "acidw", 1, 2)
	code, first, _ := openSession(t, h, v1Path("acidw", "/sessions?session_type=incremental_read"),
		`{"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"append"}}`)
	if code != http.StatusOK {
		t.Fatalf("first window code=%d", code)
	}
	insertAcidRows(t, s, "acidw", 3, 4)
	start := int64(first["LatestVersion"].(float64))
	code, second, sid2 := openSession(t, h, v1Path("acidw", "/sessions?session_type=incremental_read"),
		fmt.Sprintf(`{"IncrementalReadOptions":{"StartVersion":%d,"EndVersion":-1,"Mode":"append"}}`, start))
	if code != http.StatusOK || sid2 == "" {
		t.Fatalf("second window: code=%d body=%v", code, second)
	}
	if count, _ := second["RecordCount"].(float64); count != 2 {
		t.Fatalf("second window events=%v want 2", second["RecordCount"])
	}
	if version, _ := second["LatestVersion"].(float64); int64(version) != start+2 {
		t.Fatalf("second LatestVersion=%v want %d", second["LatestVersion"], start+2)
	}
	if _, ops := readEvents(t, h, "acidw", sid2); len(ops) != 2 {
		t.Fatalf("second window events=%v", ops)
	}
}

// TestStorageV1IncrementalRejectsUnsupportedForms keeps the declared gaps loud: a
// timestamp window, a transaction id or an unknown mode must fail, not read the table.
func TestStorageV1IncrementalRejectsUnsupportedForms(t *testing.T) {
	s, h := fixture(t, Config{})
	createAcidTable(t, s, "acidr", 1, 1)
	cases := []struct {
		name string
		body string
		code int
	}{
		{"timestamp window", `{"IncrementalReadOptions":{"StartTimeStamp":"2026-10-14 00:00:00","EndTimeStamp":"2026-10-15 00:00:00","Mode":"append"}}`, http.StatusMethodNotAllowed},
		{"txn id", `{"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"append","StartTxnId":"12"}}`, http.StatusMethodNotAllowed},
		{"unknown mode", `{"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"merge"}}`, http.StatusBadRequest},
		{"inverted range", `{"IncrementalReadOptions":{"StartVersion":5,"EndVersion":1,"Mode":"append"}}`, http.StatusBadRequest},
		{"filter pushdown", `{"FilterPredicate":"id=1","IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"append"}}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		code, body, sid := openSession(t, h, v1Path("acidr", "/sessions?session_type=incremental_read"), tc.body)
		if code != tc.code || sid != "" {
			t.Fatalf("%s: code=%d want %d body=%v", tc.name, code, tc.code, body)
		}
	}
}

// TestStorageV2IncrementalFlag covers the other public surface: the storage-api form
// reaches the same session core and reports the same version.
func TestStorageV2IncrementalFlag(t *testing.T) {
	s, h := fixture(t, Config{})
	createAcidTable(t, s, "acidv2", 1, 3)
	code, out, sid := openSession(t, h, "/api/storage/v2?Action=TableCreateReadSession&Target=projects.p.schemas.default.tables.acidv2",
		`{"IncrementalRead":true,"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"cdc"}}`)
	if code != http.StatusOK || sid == "" {
		t.Fatalf("v2 create: code=%d body=%v", code, out)
	}
	if version, _ := out["LatestVersion"].(float64); version != 3 {
		t.Fatalf("v2 LatestVersion=%v", out["LatestVersion"])
	}
	schema, _ := out["DataSchema"].(map[string]any)
	names, _ := eventColumns(t, schema)
	if len(names) != 2 {
		t.Fatalf("v2 system columns=%v", names)
	}
	payload := readV2Split(t, h, "acidv2", sid)
	reader, err := ipc.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("v2 read payload: %v %s", err, payload)
	}
	rows := 0
	for reader.Next() {
		rows += int(reader.RecordBatch().NumRows())
	}
	reader.Release()
	if rows != 3 {
		t.Fatalf("v2 event rows=%d want 3", rows)
	}
}

// TestStorageV1IncrementalCDCImages checks a CDC window really carries both sides of
// a replacement, in commit order, and that append mode does not show the old image.
func TestStorageV1IncrementalCDCImages(t *testing.T) {
	s, h := fixture(t, Config{})
	createAcidTable(t, s, "acidcdc", 1, 1)
	if err := s.Engine.Write(context.Background(), "p", "default", "acidcdc", map[string]string{},
		[][]any{{int64(1), "replaced"}}, false, []byte{'U'}); err != nil {
		t.Fatal(err)
	}
	_, cdc, sid := openSession(t, h, v1Path("acidcdc", "/sessions?session_type=incremental_read"),
		`{"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"cdc"}}`)
	if count, _ := cdc["RecordCount"].(float64); count != 3 {
		t.Fatalf("cdc events=%v want insert + before + after", cdc["RecordCount"])
	}
	_, ops := readEvents(t, h, "acidcdc", sid)
	want := []int64{2, 16, 8}
	if len(ops) != len(want) {
		t.Fatalf("cdc ops=%v want %v", ops, want)
	}
	for i := range want {
		if ops[i] != want[i] {
			t.Fatalf("cdc order=%v want %v", ops, want)
		}
	}
	_, appendOut, appendSID := openSession(t, h, v1Path("acidcdc", "/sessions?session_type=incremental_read"),
		`{"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"append"}}`)
	if count, _ := appendOut["RecordCount"].(float64); count != 2 {
		t.Fatalf("append events=%v want the two live versions", appendOut["RecordCount"])
	}
	_, appendOps := readEvents(t, h, "acidcdc", appendSID)
	for _, op := range appendOps {
		if op == 16 || op == 4 {
			t.Fatalf("append mode leaked image event %v", op)
		}
	}
}

// TestCapabilitiesDeclareIncrementalRead keeps the declared surface honest: what the
// emulator now answers, and the three forms it still refuses, both have to be listed.
func TestCapabilitiesDeclareIncrementalRead(t *testing.T) {
	_, h := fixture(t, Config{})
	code, _, body := request(t, h, "GET", "/capabilities", "")
	if code != http.StatusOK {
		t.Fatalf("capabilities code=%d", code)
	}
	text := string(body)
	for _, want := range []string{`"storage_incremental_read":["version-range","append","cdc"]`} {
		if !strings.Contains(text, want) {
			t.Fatalf("capabilities missing %s: %s", want, text)
		}
	}
	for _, gap := range []string{"incremental-timestamp-window", "incremental-txn-id", "filter-predicate-pushdown"} {
		if !strings.Contains(text, gap) {
			t.Fatalf("capabilities missing declared gap %s: %s", gap, text)
		}
	}
}

// readV2Split pulls one split through the action-style v2 surface, which is POST-only.
func readV2Split(t *testing.T, h *httptest.Server, table, sid string) []byte {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.URL+"/api/storage/v2?Action=TableRead&Target=projects.p.schemas.default.tables."+
		table+"&SessionId="+sid+"&Index=0", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "ZSTD")
	resp, err := h.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("v2 read code=%d body=%s", resp.StatusCode, raw)
	}
	return raw
}
