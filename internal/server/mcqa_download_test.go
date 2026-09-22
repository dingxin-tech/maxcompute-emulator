package server

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
)

// downloadPath is the request com.aliyun.odps.tunnel.InstanceTunnel makes for one sub
// query when an interactive executor reads its result: no download id, because there is
// no download session to create first.
func downloadPath(id string, queryID int, extra string) string {
	path := "/projects/p/instances/" + id + "?data&cached&schema_in_stream&taskname=" + sessionTask
	if queryID >= 0 {
		path += "&queryid=" + strconv.Itoa(queryID)
	}
	if extra != "" {
		path += "&" + extra
	}
	return path
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// readSchemaFrame parses the in-stream schema frame the way
// ProtobufRecordStreamReader#readSchema does, checksum included, and returns the columns
// it names plus the record stream that follows. The tag and magic numbers are spelled out
// from the SDK (field 1 / SCHEMA_END_TAG 2^25-512) rather than imported from our own
// encoder, so this fails if the implementation drifts.
func readSchemaFrame(t *testing.T, b []byte) ([]engine.Column, []byte) {
	t.Helper()
	tag, used := binary.Uvarint(b)
	if tag != 1<<3|2 {
		t.Fatalf("stream must open with the schema field (tag 0x0a), got tag %d in % x", tag, b[:min(len(b), 6)])
	}
	n, nUsed := binary.Uvarint(b[used:])
	raw := b[used+nUsed : used+nUsed+int(n)]
	end, endUsed := binary.Uvarint(b[used+nUsed+int(n):])
	if end>>3 != 33553920 {
		t.Fatalf("schema end tag=%d want 33553920", end>>3)
	}
	sum, sumUsed := binary.Uvarint(b[used+nUsed+int(n)+endUsed:])
	// The reader CRCs the field number as a 4-byte little-endian int, then the payload;
	// the tag and the length varint are outside the checksum.
	crc := crc32.Update(0, castagnoli, binary.LittleEndian.AppendUint32(nil, 1))
	crc = crc32.Update(crc, castagnoli, raw)
	if uint32(sum) != crc {
		t.Fatalf("schema frame checksum=%d want %d", uint32(sum), crc)
	}
	var envelope struct {
		Columns       []engine.Column `json:"columns"`
		PartitionKeys []engine.Column `json:"partitionKeys"`
	}
	if e := json.Unmarshal(raw, &envelope); e != nil {
		t.Fatalf("schema frame is not the JSON the SDK parses: %v / %s", e, raw)
	}
	if len(envelope.PartitionKeys) != 0 {
		t.Fatalf("a sub query result carries no partition keys: %s", raw)
	}
	for i := range envelope.Columns {
		parsed, e := engine.ParseType(envelope.Columns[i].Type)
		if e != nil {
			t.Fatalf("schema column %s has type %q: %v", envelope.Columns[i].Name, envelope.Columns[i].Type, e)
		}
		envelope.Columns[i].Parsed = parsed
	}
	return envelope.Columns, b[used+nUsed+int(n)+endUsed+sumUsed:]
}

// readRecords decodes a direct-download response into rows. wire.DecodeProtobuf re-checks
// every record CRC and the stream footer, so a header that is off by a byte shows up here
// instead of as a silently short result.
func readRecords(t *testing.T, body []byte) (engine.Result, []engine.Column) {
	t.Helper()
	cols, rest := readSchemaFrame(t, body)
	res, e := wire.DecodeProtobuf(rest, cols)
	if e != nil {
		t.Fatalf("decode records: %v (%d bytes of stream)", e, len(body))
	}
	res.Columns = cols
	return res, cols
}

func directDownloadJSONError(t *testing.T, code int, body []byte) (string, string) {
	t.Helper()
	// A tunnel client parses a JSON error body; the instance XML error shape would leave
	// it with no error code to route on.
	var e struct{ Code, Message string }
	if u := json.Unmarshal(body, &e); u != nil {
		t.Fatalf("download error is not tunnel JSON (code=%d): %v / %s", code, u, body)
	}
	if e.Code == "" {
		t.Fatalf("download error has no Code: %s", body)
	}
	return e.Code, e.Message
}

// TestMCQADirectDownloadServesTypedRows is the acceptance point for M-B's B3: the same
// statement answered by the session must come back over the instance tunnel with real
// types. The information channel can only ever hand the client strings, so a read that
// falls back to CSV would fail the int64 assertion rather than pass by accident.
func TestMCQADirectDownloadServesTypedRows(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	queryID, _ := submitStatement(t, h, id, "select id, s from t order by id limit 3")
	code, header, body := mcqaCall(t, h, "GET", downloadPath(id, queryID, ""), nil)
	if code != 200 {
		t.Fatalf("direct download: code=%d body=%s", code, body)
	}
	if got := header.Get("odps-tunnel-record-count"); got != "3" {
		t.Fatalf("record count header=%q want 3", got)
	}
	res, cols := readRecords(t, body)
	if len(cols) != 2 || cols[0].Name != "id" || cols[1].Name != "s" {
		t.Fatalf("schema columns: %+v", cols)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("rows: %d", len(res.Rows))
	}
	if v, ok := res.Rows[0][0].(int64); !ok || v != 0 {
		t.Fatalf("id must arrive as a typed bigint, got %#v", res.Rows[0][0])
	}
	if v, ok := res.Rows[2][1].(string); !ok || !strings.HasPrefix(v, "valuevalue") {
		t.Fatalf("s must arrive as a string, got %#v", res.Rows[2][1])
	}
	// Both reads describe the same result, so a client that switches paths cannot see a
	// different row count.
	csv := getInfo(t, h, infoPath(id, sessionTask, "result_"+strconv.Itoa(queryID)))
	if lines := strings.Split(strings.TrimRight(csv.Result, "\r\n"), "\r\n"); len(lines) != 4 {
		t.Fatalf("information channel rows and tunnel rows disagree: %q", csv.Result)
	}
}

// TestMCQADirectDownloadPagesWithRowRange pins the paging contract: rowrange selects a
// window, and the count the header reports is what is left to read from that window's start
// (the requested count at most). Those two numbers are all SessionRecordSetIterator has to
// walk a result: report the total for an offset read and it keeps asking for rows that are
// not there, report the window and it stops early.
func TestMCQADirectDownloadPagesWithRowRange(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	queryID, _ := submitStatement(t, h, id, "select id, s from t order by id limit 3")
	for _, tc := range []struct {
		rangeText string
		want      []int64
		count     string
	}{
		{"", []int64{0, 1, 2}, "3"},
		{"rowrange=(0,1)", []int64{0}, "1"},
		{"rowrange=(1,-1)", []int64{1, 2}, "2"},
		{"rowrange=(2,10)", []int64{2}, "1"},
		{"rowrange=(3,10)", []int64{}, "0"},
	} {
		code, header, body := mcqaCall(t, h, "GET", downloadPath(id, queryID, tc.rangeText), nil)
		if code != 200 {
			t.Fatalf("%s: code=%d body=%s", tc.rangeText, code, body)
		}
		if got := header.Get("odps-tunnel-record-count"); got != tc.count {
			t.Fatalf("%s: record count header=%q want %q", tc.rangeText, got, tc.count)
		}
		res, _ := readRecords(t, body)
		var got []int64
		for _, row := range res.Rows {
			v, _ := row[0].(int64)
			got = append(got, v)
		}
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Fatalf("%s: rows=%v want %v", tc.rangeText, got, tc.want)
		}
	}
	// A range that starts past the end is a client error, and a malformed one is too —
	// neither may come back as a valid empty stream.
	for _, bad := range []string{"rowrange=(4,10)", "rowrange=3,10", "rowrange=(x,1)", "rowrange=(-1,1)"} {
		code, body := func() (int, []byte) {
			c, _, b := mcqaCall(t, h, "GET", downloadPath(id, queryID, bad), nil)
			return c, b
		}()
		if code != 400 {
			t.Fatalf("%s: code=%d body=%s", bad, code, body)
		}
		if got, _ := directDownloadJSONError(t, code, body); got != "InvalidParameter" {
			t.Fatalf("%s: error code=%s want InvalidParameter", bad, got)
		}
	}
}

// TestMCQADirectDownloadHonoursSizeLimit keeps a byte-limited read inside the limit: the
// SDK turns an oversized response into a client-side error, so the server has to stop
// early — but never at zero rows, or a reader could not advance.
func TestMCQADirectDownloadHonoursSizeLimit(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	queryID, _ := submitStatement(t, h, id, "select id, s from t order by id limit 50")
	_, _, full := mcqaCall(t, h, "GET", downloadPath(id, queryID, ""), nil)
	for _, tc := range []struct{ limit, maxRows, minRows int }{
		{len(full), 50, 50},    // the whole stream fits, so nothing is dropped
		{len(full) / 2, 49, 1}, // half of it: fewer rows, still progress
		{1, 1, 1},              // smaller than one record: one row is sent anyway
	} {
		code, header, body := mcqaCall(t, h, "GET", downloadPath(id, queryID, "sizelimit="+strconv.Itoa(tc.limit)), nil)
		if code != 200 {
			t.Fatalf("sizelimit=%d: code=%d body=%s", tc.limit, code, body)
		}
		// A byte limit shortens this response, not the result: the client is told how many
		// rows are still to come and keeps reading.
		if got := header.Get("odps-tunnel-record-count"); got != "50" {
			t.Fatalf("sizelimit=%d: record count header=%q want the 50 rows from offset 0", tc.limit, got)
		}
		res, _ := readRecords(t, body)
		if len(res.Rows) > tc.maxRows || len(res.Rows) < tc.minRows {
			t.Fatalf("sizelimit=%d sent %d rows, want between %d and %d (body=%d bytes)",
				tc.limit, len(res.Rows), tc.minRows, tc.maxRows, len(body))
		}
	}
}

// TestMCQADirectDownloadLimitedFlagCapsRows covers READ_TABLE_MAX_ROW. The cap belongs to
// the read that asked for it: the sub query keeps its full result, so an unlimited read of
// the same sub query still returns every row.
func TestMCQADirectDownloadLimitedFlagCapsRows(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	// 2048 fixture rows, doubled three times: 16384, i.e. past the 10000 cap.
	for n := 0; n < 3; n++ {
		if _, ir := submitStatement(t, h, id, fmt.Sprintf("insert into t select id+%d, s from t", n+1)); ir.Status != mcqaInfoOK {
			t.Fatalf("grow the table: %+v", ir)
		}
	}
	queryID, _ := submitStatement(t, h, id, "select id from t")
	for _, tc := range []struct {
		extra string
		want  string
		rows  int
	}{{"", "16384", 16384}, {"instance_tunnel_limit_enabled", "10000", 10000}} { //nolint:dupl
		code, header, body := mcqaCall(t, h, "GET", downloadPath(id, queryID, tc.extra), nil)
		if code != 200 {
			t.Fatalf("%q: code=%d body=%s", tc.extra, code, body)
		}
		if got := header.Get("odps-tunnel-record-count"); got != tc.want {
			t.Fatalf("%q: record count header=%q want %q", tc.extra, got, tc.want)
		}
		res, _ := readRecords(t, body)
		if len(res.Rows) != tc.rows {
			t.Fatalf("%q: %d rows want %d", tc.extra, len(res.Rows), tc.rows)
		}
	}
	// The paging loop must stop at the cap instead of walking the rest of the result.
	code, header, body := mcqaCall(t, h, "GET", downloadPath(id, queryID, "instance_tunnel_limit_enabled&rowrange=(9999,1)"), nil)
	if code != 200 {
		t.Fatalf("limited read at the cap: code=%d body=%s", code, body)
	}
	if got := header.Get("odps-tunnel-record-count"); got != "1" {
		t.Fatalf("a limited read one row before the cap reports %q rows left", got)
	}
	res, _ := readRecords(t, body)
	if len(res.Rows) != 1 {
		t.Fatalf("limited read at row 9999 sent %d rows", len(res.Rows))
	}
	// The cap is a hard boundary: one row past it is the empty stream a paging client
	// stops on, and a start beyond it is the range error the offline download path gives.
	if code, header, body = mcqaCall(t, h, "GET", downloadPath(id, queryID, "instance_tunnel_limit_enabled&rowrange=(10000,1)"), nil); code != 200 {
		t.Fatalf("a limited read at the cap: code=%d body=%s", code, body)
	} else if got := header.Get("odps-tunnel-record-count"); got != "0" {
		t.Fatalf("a limited read at the cap reports %q rows left", got)
	} else if res, _ := readRecords(t, body); len(res.Rows) != 0 {
		t.Fatalf("a limited read served %d rows past the cap", len(res.Rows))
	}
	if code, _, _ = mcqaCall(t, h, "GET", downloadPath(id, queryID, "instance_tunnel_limit_enabled&rowrange=(10001,1)"), nil); code != 400 {
		t.Fatalf("a limited read starting past the cap must be a range error")
	}
}

// TestMCQADirectDownloadRefusesNonSelect matches the service, which will not tunnel a
// statement without a result set. The code and message are the pair
// SQLExecutorImpl#checkIsSelect recognises; anything else would be read as a select that
// happened to come back empty.
func TestMCQADirectDownloadRefusesNonSelect(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	for _, statement := range []string{"create table ns(a bigint)", "insert into t values (9001,'x')"} {
		queryID, ir := submitStatement(t, h, id, statement)
		if ir.Status != mcqaInfoOK {
			t.Fatalf("%s: %+v", statement, ir)
		}
		code, _, body := mcqaCall(t, h, "GET", downloadPath(id, queryID, ""), nil)
		if code != 400 {
			t.Fatalf("%s: code=%d body=%s", statement, code, body)
		}
		got, message := directDownloadJSONError(t, code, body)
		if got != "InstanceTypeNotSupported" || !strings.Contains(message, "Non select query not supported") {
			t.Fatalf("%s: code=%s message=%s", statement, got, message)
		}
	}
	// A select with no rows is not a non-select: it answers with a schema and zero records.
	queryID, _ := submitStatement(t, h, id, "select id, s from t where id = 99999")
	code, header, body := mcqaCall(t, h, "GET", downloadPath(id, queryID, ""), nil)
	if code != 200 || header.Get("odps-tunnel-record-count") != "0" {
		t.Fatalf("empty select: code=%d count=%q body=%s", code, header.Get("odps-tunnel-record-count"), body)
	}
	res, cols := readRecords(t, body)
	if len(res.Rows) != 0 || len(cols) != 2 {
		t.Fatalf("empty select lost its schema: %d rows, %d columns", len(res.Rows), len(cols))
	}
}

// TestMCQADirectDownloadFailuresAreStructured keeps every rejection distinguishable: a
// wrong task, an unknown sub query and a one-shot instance are three different problems,
// and an empty 200 would look like "the query returned nothing" to all three.
func TestMCQADirectDownloadFailuresAreStructured(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	queryID, _ := submitStatement(t, h, id, "select id from t limit 1")
	wrongTask := "/projects/p/instances/" + id + "?data&cached&taskname=other_task&queryid=" + strconv.Itoa(queryID)
	if code, _, body := mcqaCall(t, h, "GET", wrongTask, nil); code != 400 {
		t.Fatalf("unknown task name: code=%d body=%s", code, body)
	} else if got, _ := directDownloadJSONError(t, code, body); got != "InvalidParameter" {
		t.Fatalf("unknown task name code=%s", got)
	}
	if code, _, body := mcqaCall(t, h, "GET", downloadPath(id, 999, ""), nil); code != 404 {
		t.Fatalf("unknown sub query: code=%d body=%s", code, body)
	} else if got, message := directDownloadJSONError(t, code, body); got != "NoSuchInstance" || !strings.Contains(message, "999") {
		t.Fatalf("unknown sub query: code=%s message=%s", got, message)
	}
	// A failed statement is not an empty result either: the SDK re-runs a select whose
	// tunnel read failed, and the error text is what the user ends up seeing.
	failed, _ := submitStatement(t, h, id, "select * from missing_table")
	code, _, body := mcqaCall(t, h, "GET", downloadPath(id, failed, ""), nil)
	if code != 409 {
		t.Fatalf("failed sub query: code=%d body=%s", code, body)
	}
	if got, message := directDownloadJSONError(t, code, body); got != "InvalidState" || !strings.Contains(message, "missing_table") {
		t.Fatalf("failed sub query: code=%s message=%s", got, message)
	}
	// A one-shot instance has no sub queries; saying InvalidState is what lets the client
	// use its documented download path instead.
	code, header, body := mcqaCall(t, h, "POST", "/projects/p/instances",
		[]byte(`<Instance><Job><Tasks><SQL><Name>oneshot</Name><Query>select 1</Query></SQL></Tasks></Job></Instance>`))
	if code != 201 {
		t.Fatalf("one-shot create: code=%d body=%s", code, body)
	}
	oneShot := strings.TrimPrefix(header.Get("Location"), "/projects/p/instances/")
	path := "/projects/p/instances/" + oneShot + "?data&cached&taskname=oneshot"
	if code, _, body := mcqaCall(t, h, "GET", path, nil); code != 409 {
		t.Fatalf("direct download of a one-shot instance: code=%d body=%s", code, body)
	} else if got, message := directDownloadJSONError(t, code, body); got != "InvalidState" || !strings.Contains(message, "downloads") {
		t.Fatalf("one-shot instance download: code=%s message=%s", got, message)
	}
	// Unknown instances stay the plain not-found they have always been.
	if code, _, _ := mcqaCall(t, h, "GET", downloadPath("missing", 1, ""), nil); code != 404 {
		t.Fatalf("missing instance: code=%d", code)
	}
}

// TestMCQADirectDownloadLongPollsARunningSubQuery is why the read must not answer an empty
// stream while a statement is in flight: the SDK's "cached" read is a long poll, and a
// premature empty answer is indistinguishable from a query that returned nothing.
func TestMCQADirectDownloadLongPollsARunningSubQuery(t *testing.T) {
	s, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	queryID, _ := submitStatement(t, h, id, "select id from t limit 2")
	setStatus := func(status int) {
		s.mu.Lock()
		defer s.mu.Unlock()
		q := s.instances[id].MCQA.queries[queryID]
		if q == nil {
			t.Fatalf("sub query %d is gone from the session", queryID)
		}
		q.Status = status
	}
	// The emulator finishes a statement before it answers the submission, so an in-flight
	// sub query is only reachable by a client that reads without polling first — or by a
	// test that puts the session back into that state.
	setStatus(mcqaStatusRunning)
	done := make(chan *http.Response, 1)
	go func() {
		res, e := http.Get(h.URL + downloadPath(id, queryID, ""))
		if e != nil {
			t.Errorf("long poll: %v", e)
			close(done)
			return
		}
		done <- res
	}()
	select {
	case res := <-done:
		body, _ := io.ReadAll(res.Body)
		t.Fatalf("a running sub query answered immediately: code=%d body=%s", res.StatusCode, body)
	case <-time.After(100 * time.Millisecond):
	}
	setStatus(mcqaStatusTerminated)
	var res *http.Response
	select {
	case res = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the read never came back after the sub query finished")
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("long poll after completion: code=%d", res.StatusCode)
	}
	body, e := io.ReadAll(res.Body)
	if e != nil {
		t.Fatal(e)
	}
	if got := res.Header.Get("odps-tunnel-record-count"); got != "2" {
		t.Fatalf("record count header=%q want 2", got)
	}
	if decoded, _ := readRecords(t, body); len(decoded.Rows) != 2 {
		t.Fatalf("long poll rows: %d", len(decoded.Rows))
	}
	// A cancelled sub query answers with the flag the SDK rethrows instead of re-running.
	setStatus(mcqaStatusCancelled)
	code, _, body := mcqaCall(t, h, "GET", downloadPath(id, queryID, ""), nil)
	if code != 409 {
		t.Fatalf("cancelled sub query: code=%d body=%s", code, body)
	}
	if _, message := directDownloadJSONError(t, code, body); !strings.Contains(message, "Job is cancelled") {
		t.Fatalf("cancelled sub query message=%s", message)
	}
}

// TestMCQADirectDownloadKeepsTheSessionAlive: a result read is session activity, the same
// way an information call is. Otherwise a client that polls results through the tunnel and
// only submits every now and then would have its session dropped underneath it.
func TestMCQADirectDownloadKeepsTheSessionAlive(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: 250 * time.Millisecond})
	id := createSession(t, h, "")
	queryID, _ := submitStatement(t, h, id, "select id from t limit 1")
	for n := 0; n < 2; n++ {
		time.Sleep(150 * time.Millisecond)
		code, _, body := mcqaCall(t, h, "GET", downloadPath(id, queryID, ""), nil)
		if code != 200 {
			t.Fatalf("read %d after %dms of tunnel activity: code=%d body=%s", n, 150*(n+1), code, body)
		}
	}
	time.Sleep(300 * time.Millisecond)
	code, _, body := mcqaCall(t, h, "GET", downloadPath(id, queryID, ""), nil)
	if code != 404 {
		t.Fatalf("an idle session must still expire: code=%d body=%s", code, body)
	}
}

// TestMCQADirectDownloadFollowsRetention: the tunnel must not keep serving a result the
// session has already dropped, or a client would read rows the information channel has
// reported as gone.
func TestMCQADirectDownloadFollowsRetention(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	first, _ := submitStatement(t, h, id, "select id from t limit 2")
	for n := 0; n < mcqaRetainedQueries; n++ {
		if _, ir := submitStatement(t, h, id, "select 1"); ir.Status != mcqaInfoOK {
			t.Fatalf("fill the retention window: %+v", ir)
		}
	}
	if r := getInfo(t, h, infoPath(id, sessionTask, "result_"+strconv.Itoa(first))); r.Status != mcqaStatusFailed {
		t.Fatalf("the information channel kept a dropped sub query: %+v", r)
	}
	code, _, body := mcqaCall(t, h, "GET", downloadPath(id, first, ""), nil)
	if code != 404 {
		t.Fatalf("a dropped sub query is still downloadable: code=%d body=%s", code, body)
	}
}

// TestProtobufSchemaTailMatchesPlainStream pins the claim in the encoder's comment: only
// the schema frame is added, so the record path stays byte-identical to the one the
// download sessions already serve.
func TestProtobufSchemaTailMatchesPlainStream(t *testing.T) {
	id, e := engine.ParseType("bigint")
	if e != nil {
		t.Fatal(e)
	}
	res := engine.Result{Columns: []engine.Column{{Name: "id", Type: "bigint", Nullable: true, Parsed: id}}, Rows: [][]any{{int64(1)}, {nil}, {int64(3)}}}
	withSchema, e := wire.ProtobufSchema(res)
	if e != nil {
		t.Fatal(e)
	}
	plain, e := wire.Protobuf(res)
	if e != nil {
		t.Fatal(e)
	}
	cols, rest := readSchemaFrame(t, withSchema)
	if cols[0].Name != "id" || cols[0].Type != "bigint" {
		t.Fatalf("schema frame columns: %+v", cols)
	}
	if !bytes.Equal(rest, plain) {
		t.Fatalf("record stream after the schema frame differs from wire.Protobuf")
	}
}
