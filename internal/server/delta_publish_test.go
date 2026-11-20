package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
)

// TestTunnelStreamPublishesCDCEvents covers the P1 review finding through the public
// surface that hands the engine rows with no operation bytes at all: an ordinary Tunnel
// stream upload (upload.go, kind == "stream"). The live row was inserted and the version
// bound advanced, while the event history stayed empty - so a CDC consumer that read the
// window, found nothing, and moved its cursor forward lost the row permanently.
func TestTunnelStreamPublishesCDCEvents(t *testing.T) {
	s, h := fixture(t, Config{})
	ctx := context.Background()
	if _, err := s.Engine.Execute(ctx, "p", "default", "create table tc(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"); err != nil {
		t.Fatal(err)
	}
	c1, _ := engine.NewColumn("id", "bigint")
	c2, _ := engine.NewColumn("s", "string")
	payload, err := wire.Protobuf(engine.Result{Columns: []engine.Column{c1, c2}, Rows: [][]any{{int64(1), "a"}, {int64(2), "b"}}})
	if err != nil {
		t.Fatal(err)
	}
	code, b := sendBody(t, h, "POST", "/projects/p/tables/tc/streams", nil, nil)
	if code != 200 {
		t.Fatalf("create stream %d %s", code, b)
	}
	var m map[string]any
	if err = json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	name, _ := m["session_name"].(string)
	if name == "" {
		t.Fatalf("no session_name in %s", b)
	}
	if code, b = sendBody(t, h, "PUT", "/projects/p/tables/tc/streams?uploadid="+name, payload, nil); code != 200 {
		t.Fatalf("stream write %d %s", code, b)
	}
	_, sid := cdcSession(t, h, "tc", 0)
	fields, ops := readEvents(t, h, "tc", sid)
	if len(ops) != 2 || ops[0] != 2 || ops[1] != 2 {
		t.Fatalf("after a tunnel stream write events=%v fields=%v want two inserts", ops, fields)
	}
	// Reading the same window again returns the same two events: publishing on retry or
	// re-read must not duplicate or drop anything.
	if _, again := readEvents(t, h, "tc", cdcSid(t, h, "tc", 0)); len(again) != 2 {
		t.Fatalf("re-read events=%v want the same two inserts", again)
	}
	_, latest, err := s.Engine.DeltaRead(ctx, "p", "default", "tc", nil, 0, -1, true)
	if err != nil || latest < 2 {
		t.Fatalf("latest=%d err=%v", latest, err)
	}
	if _, after := readEvents(t, h, "tc", cdcSid(t, h, "tc", latest)); len(after) != 0 {
		t.Fatalf("window above the cursor events=%v want none", after)
	}
}

// cdcSession opens a v1 CDC session from a start version and returns its id.
func cdcSession(t *testing.T, h *httptest.Server, table string, from int64) (int, string) {
	t.Helper()
	code, out, sid := openSession(t, h, v1Path(table, "/sessions?session_type=incremental_read"),
		`{"IncrementalReadOptions":{"StartVersion":`+strconv.FormatInt(from, 10)+`,"EndVersion":-1,"Mode":"cdc"}}`)
	if code != 200 || sid == "" {
		t.Fatalf("cdc session from %d: code=%d body=%v", from, code, out)
	}
	return code, sid
}

func cdcSid(t *testing.T, h *httptest.Server, table string, from int64) string {
	t.Helper()
	_, sid := cdcSession(t, h, table, from)
	return sid
}

// TestOrdinaryTunnelBlockCommitPublishesCDCEvents covers the surface the review named by
// name - an ordinary Tunnel upload (block commit, not an upsert session, so the server
// hands the engine no operation bytes at all) - followed by a CDC read.
func TestOrdinaryTunnelBlockCommitPublishesCDCEvents(t *testing.T) {
	s, h := fixture(t, Config{})
	ctx := context.Background()
	if _, err := s.Engine.Execute(ctx, "p", "default", "create table tb(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"); err != nil {
		t.Fatal(err)
	}
	c1, _ := engine.NewColumn("id", "bigint")
	c2, _ := engine.NewColumn("s", "string")
	payload, err := wire.Protobuf(engine.Result{Columns: []engine.Column{c1, c2}, Rows: [][]any{{int64(1), "a"}, {int64(2), "b"}}})
	if err != nil {
		t.Fatal(err)
	}
	code, b := sendBody(t, h, "POST", "/projects/p/tables/tb?uploads", nil, nil)
	if code != 200 {
		t.Fatalf("create upload %d %s", code, b)
	}
	var m map[string]any
	if err = json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	upload, _ := m["UploadID"].(string)
	if upload == "" {
		t.Fatalf("no upload id in %s", b)
	}
	if code, b = sendBody(t, h, "PUT", "/projects/p/tables/tb?uploadid="+upload+"&blockid=0", payload, nil); code != 200 {
		t.Fatalf("block write %d %s", code, b)
	}
	if code, b = sendBody(t, h, "POST", "/projects/p/tables/tb?uploadid="+upload, nil, nil); code != 200 {
		t.Fatalf("commit %d %s", code, b)
	}
	_, sid := cdcSession(t, h, "tb", 0)
	if fields, ops := readEvents(t, h, "tb", sid); len(ops) != 2 || ops[0] != 2 || ops[1] != 2 {
		t.Fatalf("after a block commit events=%v fields=%v want two inserts", ops, fields)
	}
}

// TestStorageBatchWriteNeedsOperationColumn is the falsifiable reading behind the claim in
// the review reply: a write session on a primary-key table always carries the operation
// column, so that surface reaches the engine with ops != nil and was never in the broken
// set. Two directions are pinned: a payload without the column is refused on schema
// grounds, and a payload with it publishes the CDC event the consumer would otherwise lose.
func TestStorageBatchWriteNeedsOperationColumn(t *testing.T) {
	s, h := fixture(t, Config{})
	ctx := context.Background()
	if _, err := s.Engine.Execute(ctx, "p", "default", "create table td(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"); err != nil {
		t.Fatal(err)
	}
	c1, _ := engine.NewColumn("id", "bigint")
	c2, _ := engine.NewColumn("s", "string")
	c3, _ := engine.NewColumn("__operation", "tinyint")
	base := "/api/storage/v2?Target=projects.p.schemas.default.tables.td&WriteMode=Batch&Action="
	code, b := sendBody(t, h, "POST", base+"TableCreateWriteSession", []byte(`{}`), nil)
	if code != 200 {
		t.Fatal(code, string(b))
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	sid, _ := m["SessionId"].(string)
	if code, b = sendBody(t, h, "POST", base+"TableCreateWriteStream&SessionId="+sid, []byte(`{"StreamId":"s1","StreamVersion":1}`), nil); code != 200 {
		t.Fatal(code, string(b))
	}
	// Without the operation column the write is refused: the session expects it.
	bare, err := wire.Arrow(engine.Result{Columns: []engine.Column{c1, c2}, Rows: [][]any{{int64(1), "a"}}}, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	code, b = sendBody(t, h, "POST", base+"TableWrite&SessionId="+sid+"&StreamId=s1&StreamVersion=1&Count=1&RowOffset=0", bare, nil)
	if code == 200 {
		t.Fatalf("a payload without the operation column was accepted: %d %s", code, b)
	}
	// With it, the batch commit publishes the insert the CDC read then returns.
	withOp, err := wire.Arrow(engine.Result{Columns: []engine.Column{c1, c2, c3}, Rows: [][]any{{int64(1), "a", int64('U')}}}, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	if code, b = sendBody(t, h, "POST", base+"TableWrite&SessionId="+sid+"&StreamId=s1&StreamVersion=1&Count=1&RowOffset=0", withOp, nil); code != 200 {
		t.Fatalf("write with operation %d %s", code, b)
	}
	if code, b = sendBody(t, h, "POST", base+"TableCloseWriteStream&SessionId="+sid, []byte(`{"StreamId":"s1","StreamVersion":1}`), nil); code != 200 {
		t.Fatal(code, string(b))
	}
	if code, b = sendBody(t, h, "POST", base+"TableCommitWriteSession&SessionId="+sid, []byte(`{}`), nil); code != 200 {
		t.Fatal(code, string(b))
	}
	_, cdcSid := cdcSession(t, h, "td", 0)
	_, ops := readEvents(t, h, "td", cdcSid)
	if len(ops) == 0 {
		t.Fatalf("no CDC events after a batch commit with an insert operation")
	}
	if ops[len(ops)-1] != 2 && ops[len(ops)-1] != 32 {
		t.Fatalf("last event=%d want the insert/after-image of the committed row", ops[len(ops)-1])
	}
}
