package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
)

func sendBody(t *testing.T, h *httptest.Server, method, path string, body []byte, headers map[string]string) (int, []byte) {
	t.Helper()
	r, _ := http.NewRequest(method, h.URL+path, bytes.NewReader(body))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	res, e := h.Client().Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer res.Body.Close()
	b, e := io.ReadAll(res.Body)
	if e != nil {
		t.Fatal(e)
	}
	return res.StatusCode, b
}
func TestUploadCorruptionRetryAndBinding(t *testing.T) {
	s, h := fixture(t, Config{})
	ctx := context.Background()
	s.Engine.Execute(ctx, "p", "default", "create table writes(id bigint,s string)")
	cols := []engine.Column{}
	for _, spec := range [][2]string{{"id", "bigint"}, {"s", "string"}} {
		c, _ := engine.NewColumn(spec[0], spec[1])
		cols = append(cols, c)
	}
	payload, e := wire.Protobuf(engine.Result{Columns: cols, Rows: [][]any{{int64(7), "retry"}}})
	if e != nil {
		t.Fatal(e)
	}
	code, b := sendBody(t, h, "POST", "/projects/p/tables/writes/streams", nil, nil)
	if code != 200 {
		t.Fatalf("create %d %s", code, b)
	}
	var m map[string]any
	json.Unmarshal(b, &m)
	path := "/projects/p/tables/writes/streams?uploadid=" + m["session_name"].(string)
	hdr := map[string]string{"odps-tunnel-retry-trace-id": "same-request"}
	for n := 0; n < 2; n++ {
		code, b = sendBody(t, h, "PUT", path, payload, hdr)
		if code != 200 {
			t.Fatalf("write %d %s", code, b)
		}
	}
	corrupt := append([]byte{}, payload...)
	corrupt[len(corrupt)-1] ^= 1
	code, _ = sendBody(t, h, "PUT", path, corrupt, hdr)
	if code != 409 {
		t.Fatalf("conflicting retry: %d", code)
	}
	code, _ = sendBody(t, h, "PUT", path, corrupt, nil)
	if code != 400 {
		t.Fatalf("bad checksum: %d", code)
	}
	code, _ = sendBody(t, h, "PUT", strings.Replace(path, "/writes/", "/t/", 1), payload, nil)
	if code != 404 {
		t.Fatalf("session binding: %d", code)
	}
	data, _, e := s.Engine.Snapshot(ctx, "p", "default", "writes", map[string]string{})
	if e != nil || len(data.Rows) != 1 {
		t.Fatalf("duplicate/corrupt data: %v %v", data.Rows, e)
	}
}
func TestStorageExactlyOnceReplayAndAtomicCommit(t *testing.T) {
	s, h := fixture(t, Config{})
	ctx := context.Background()
	s.Engine.Execute(ctx, "p", "default", "create table writes(id bigint,s string)")
	base := "/api/storage/v2?Target=projects.p.schemas.default.tables.writes&WriteMode=Batch&Action="
	call := func(action string, body string) (int, []byte) {
		return sendBody(t, h, "POST", base+action, []byte(body), nil)
	}
	code, b := call("TableCreateWriteSession", `{}`)
	if code != 200 {
		t.Fatal(code, string(b))
	}
	var m map[string]any
	json.Unmarshal(b, &m)
	sid := m["SessionId"].(string)
	code, b = call("TableCreateWriteStream&SessionId="+sid, `{"StreamId":"s","StreamVersion":1,"ExactlyOnceMode":true}`)
	if code != 200 {
		t.Fatal(code, string(b))
	}
	cols := []engine.Column{}
	for _, spec := range [][2]string{{"id", "bigint"}, {"s", "string"}} {
		c, _ := engine.NewColumn(spec[0], spec[1])
		cols = append(cols, c)
	}
	payload, _ := wire.Arrow(engine.Result{Columns: cols, Rows: [][]any{{int64(7), "exact"}}}, 1, false)
	path := base + "TableWrite&SessionId=" + sid + "&StreamId=s&StreamVersion=1&Count=1&RowOffset=0"
	for i := 0; i < 2; i++ {
		code, b = sendBody(t, h, "POST", path, payload, nil)
		if code != 200 {
			t.Fatal(code, string(b))
		}
	}
	code, _ = sendBody(t, h, "POST", strings.Replace(path, "RowOffset=0", "RowOffset=2", 1), payload, nil)
	if code != 409 {
		t.Fatal("gap", code)
	}
	code, _ = call("TableCommitWriteSession&SessionId="+sid, `{"StreamIds":["s"],"StreamVersions":[1]}`)
	if code != 409 {
		t.Fatal("unclosed", code)
	}
	code, b = call("TableCloseWriteStream&SessionId="+sid, `{"StreamId":"s","StreamVersion":1}`)
	if code != 200 {
		t.Fatal(code, string(b))
	}
	for i := 0; i < 2; i++ {
		code, b = call("TableCommitWriteSession&SessionId="+sid, `{}`)
		if code != 200 {
			t.Fatal(code, string(b))
		}
	}
	data, _, e := s.Engine.Snapshot(ctx, "p", "default", "writes", map[string]string{})
	if e != nil || len(data.Rows) != 1 {
		t.Fatal(data.Rows, e)
	}
	code, _ = sendBody(t, h, "POST", path, payload, nil)
	if code != 409 {
		t.Fatal("write after commit", code)
	}
}
