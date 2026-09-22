package server

// MCQA sub-query results are also downloadable through the instance tunnel, which is the
// default way the Java SDK's interactive executor reads them:
//
//	SQLExecutorImpl#getSessionResultSetByInstanceTunnel
//	  -> InstanceTunnel#createDirectDownloadSession(project, instance, taskname, queryid, limit)
//	  -> TunnelRecordReader -> GET /projects/{p}/instances/{id}
//	       ?data&cached&schema_in_stream&taskname=..[&queryid=N]
//	        [&rowrange=(start,count)][&sizelimit=bytes][&instance_tunnel_limit_enabled]
//
// Two things differ from an offline instance download:
//
//   - There is no download session to create first, so nothing else carries the schema.
//     The body starts with the in-stream JSON schema frame that
//     ProtobufRecordStreamReader#readSchema reads (the Java SDK labels that method "for
//     MCQA direct download").
//   - `odps-tunnel-record-count` is the number of rows the read makes available from its
//     start offset, because that is what SessionRecordSetIterator iterates: it remembers
//     the first answer's count and keeps calling openRecordReader(offset+cursor,
//     fetchSize) while cursor < it. Reporting the whole result there makes a client that
//     asked for an offset read past the end of it.

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
)

const (
	// mcqaLimitRows is the service's READ_TABLE_MAX_ROW, which a client asks the tunnel
	// to apply by naming the download limited (instance_tunnel_limit_enabled).
	mcqaLimitRows = 10000
	// mcqaDirectWait bounds how long a direct read waits for a sub query that is still
	// running. Clients normally poll the information channel first; one that reads right
	// away is long polling on purpose, so the answer blocks instead of serving an empty
	// stream. The code and message are the pair the Java SDK treats as a retryable
	// tunnel timeout (SQLExecutorConstants.sessionTunnelTimeout*).
	mcqaDirectWait = 30 * time.Second
	// mcqaDirectPoll is how often a waiting read re-checks the sub query. Statements run
	// with the server lock released, so the wait must not hold it.
	mcqaDirectPoll = 5 * time.Millisecond
)

// mcqaRead is the outcome of one attempt to resolve a direct-download request.
type mcqaRead struct {
	Result  engine.Result
	Status  int
	Code    string
	Message string
	Pending bool // sub query still running: wait, then retry or time out
}

// failed reports a structured answer rather than a result stream.
func (m mcqaRead) failed() bool { return m.Code != "" }

// mcqaDirectDownload answers a session sub-query download with a record stream.
func (s *Server) mcqaDirectDownload(w http.ResponseWriter, r *http.Request, p, sc, iid string) {
	q := r.URL.Query()
	task := q.Get("taskname")
	if task == "" {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("taskname is required to download a session sub query result"))
		return
	}
	subQueryID := -1
	if raw := q.Get("queryid"); raw != "" {
		v, e := strconv.Atoi(raw)
		if e != nil {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid queryid %q", raw))
			return
		}
		subQueryID = v
	}
	start, count, e := parseRowRange(q.Get("rowrange"))
	if e != nil {
		fail(w, r, 400, "InvalidParameter", e)
		return
	}
	sizeLimit := int64(0)
	if raw := q.Get("sizelimit"); raw != "" {
		sizeLimit, e = strconv.ParseInt(raw, 10, 64)
		if e != nil || sizeLimit < 0 {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid sizelimit %q", raw))
			return
		}
	}
	limited := q.Has("instance_tunnel_limit_enabled")
	query := "the current sub query"
	if subQueryID >= 0 {
		query = "sub query " + strconv.Itoa(subQueryID)
	}
	deadline := time.Now().Add(mcqaDirectWait)
	for {
		res := s.mcqaResolveRead(p, sc, iid, task, subQueryID, limited)
		switch {
		case !res.Pending && !res.failed():
			s.writeRecordStream(w, r, res.Result, start, count, sizeLimit)
			return
		case !res.Pending:
			fail(w, r, res.Status, res.Code, fmt.Errorf("%s", res.Message))
			return
		}
		if time.Now().Add(mcqaDirectPoll).After(deadline) {
			fail(w, r, 500, "OdpsTaskTimeout", fmt.Errorf("Wait for cache data timeout: %s of session %s is still running", query, iid))
			return
		}
		if !sleepUntil(r.Context(), mcqaDirectPoll) {
			return // the client went away; there is nobody left to answer
		}
	}
}

// mcqaResolveRead looks up the sub query a download names and returns the rows it may
// serve. It runs under the server lock; statement execution takes the same lock to store
// its answer, so callers never hold it across a wait.
func (s *Server) mcqaResolveRead(p, sc, iid, task string, subQueryID int, limited bool) mcqaRead {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.instances[iid]
	if i == nil || i.Project != p || i.Schema != sc {
		return mcqaRead{Status: 404, Code: "NoSuchInstance", Message: "unknown instance"}
	}
	m := i.MCQA
	if m == nil {
		// An offline instance has no sub queries. Saying so is what lets the SDK use its
		// documented fallback instead of parsing an error out of a result stream.
		return mcqaRead{Status: 409, Code: "InvalidState", Message: "instance is not an MCQA session; download its result with ?downloads"}
	}
	if s.mcqaIdle(i) {
		delete(s.instances, i.ID)
		return mcqaRead{Status: 404, Code: "NoSuchInstance", Message: "session " + iid + " has expired"}
	}
	if task != i.Name {
		return mcqaRead{Status: 400, Code: "InvalidParameter", Message: fmt.Sprintf("session %s has no task %s", iid, task)}
	}
	// Reading a result is session activity, like the information calls are.
	m.lastActive = time.Now()
	id := m.lastID // no queryid means "the current sub query"
	if subQueryID >= 0 {
		id = subQueryID
	}
	q := m.queries[id]
	switch {
	case q == nil: // never submitted, or dropped by the retention window
		return mcqaRead{Status: 404, Code: "NoSuchInstance", Message: fmt.Sprintf("session %s has no sub query %d", iid, id)}
	case q.Status == mcqaStatusRunning:
		return mcqaRead{Pending: true}
	case q.Status == mcqaStatusCancelled:
		return mcqaRead{Status: 409, Code: "OdpsJobCancelledException", Message: fmt.Sprintf("Job is cancelled: session %s sub query %d", iid, id)}
	case q.Status == mcqaStatusFailed:
		// The failure text is the statement error. The SDK answers a tunnel failure for a
		// select by re-running it offline, where the same error surfaces again — so this
		// must not be dressed up as an empty result.
		return mcqaRead{Status: 409, Code: "InvalidState", Message: fmt.Sprintf("session %s sub query %d failed: %s", iid, id, q.Result)}
	case q.Status != mcqaStatusTerminated:
		return mcqaRead{Status: 409, Code: "InvalidState", Message: fmt.Sprintf("session %s sub query %d is not terminated", iid, id)}
	case len(q.Data.Columns) == 0:
		// The service will not tunnel a non-select statement. These two strings are the
		// ones SQLExecutorImpl#checkIsSelect looks for, after which it confirms the
		// outcome through the session API rather than reading records.
		return mcqaRead{Status: 400, Code: "InstanceTypeNotSupported", Message: "Non select query not supported by session result download"}
	}
	count := len(q.Data.Rows)
	if limited && count > mcqaLimitRows {
		// READ_TABLE_MAX_ROW applies to this read only; the sub query keeps its full
		// result, and an unlimited read of it still returns every row.
		count = mcqaLimitRows
	}
	return mcqaRead{Result: engine.Result{Columns: q.Data.Columns, Rows: q.Data.Rows[:count]}}
}

// writeRecordStream sends the rows of res that a read starting at `start` may take, as a
// schema-first record stream, honouring a byte limit by dropping trailing rows. At least
// one row is sent when the limit is smaller than a single record: a reader that cannot
// advance would otherwise stall.
func (s *Server) writeRecordStream(w http.ResponseWriter, r *http.Request, res engine.Result, start, count, sizeLimit int64) {
	total := int64(len(res.Rows))
	if start > total {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("rowrange starts at %d, the result has %d rows", start, total))
		return
	}
	// The count the client gets is what it iterates, so it has to be the rows this read
	// makes available from `start` — the remainder of the result, cut short by the
	// requested count. SessionRecordSetIterator keeps that first answer's number and
	// pages until it reaches it, so a total here would send it past the end of the
	// result looking for rows it was promised.
	available := total - start
	if count >= 0 && count < available {
		available = count
	}
	out := engine.Result{Columns: res.Columns, Rows: res.Rows[start : start+available]}
	data, e := wire.ProtobufSchema(out)
	if e != nil {
		fail(w, r, 400, "SerializationError", e)
		return
	}
	// Halving keeps the re-encodes logarithmic; dropping one row at a time would let a
	// one-byte limit turn a wide result into thousands of encodes.
	for n := len(out.Rows); sizeLimit > 0 && int64(len(data)) > sizeLimit && n > 1; {
		n = max(1, n/2)
		out.Rows = out.Rows[:n]
		if data, e = wire.ProtobufSchema(out); e != nil {
			fail(w, r, 400, "SerializationError", e)
			return
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("odps-tunnel-record-count", strconv.FormatInt(available, 10))
	w.WriteHeader(200)
	w.Write(data)
}

// parseRowRange reads the tunnel rowrange parameter. An empty value means "from the start,
// all rows"; the SDK sends a negative count for "no limit" and Long.MAX_VALUE when it was
// given only a start offset.
func parseRowRange(text string) (start, count int64, e error) {
	if text == "" {
		return 0, -1, nil
	}
	if !strings.HasPrefix(text, "(") || !strings.HasSuffix(text, ")") {
		return 0, 0, fmt.Errorf("rowrange=(start,count) required")
	}
	a := strings.Split(strings.TrimSuffix(strings.TrimPrefix(text, "("), ")"), ",")
	if len(a) != 2 {
		return 0, 0, fmt.Errorf("invalid rowrange %q", text)
	}
	if start, e = strconv.ParseInt(a[0], 10, 64); e != nil || start < 0 {
		return 0, 0, fmt.Errorf("invalid rowrange %q", text)
	}
	if count, e = strconv.ParseInt(a[1], 10, 64); e != nil {
		return 0, 0, fmt.Errorf("invalid rowrange %q", text)
	}
	if count < 0 {
		count = -1
	}
	return start, count, nil
}

// sleepUntil waits for d and reports whether it finished, bailing out early when the
// request is cancelled.
func sleepUntil(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
