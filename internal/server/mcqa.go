package server

// MCQA / SQLRT interactive sessions.
//
// The wire contract implemented here is the one `com.aliyun.odps.Session` and
// `com.aliyun.odps.Instance#getTaskInfo` / `#setInformation` put on the wire in the
// public Java SDK (odps-sdk-core), namely:
//
//	POST /projects/{p}/instances                    Instance/Job/Tasks/SQLRT -> a session
//	GET  .../instances/{id}?info&taskname=&key=     -> SubQueryResponse JSON
//	PUT  .../instances/{id}?info&taskname=          -> InstanceTaskInfo XML in,
//	                                                   SetInformationResult JSON out
//	PUT  .../instances/{id}                          -> Instance/Status=Terminated stops it
//
// A session is an instance that stays Running until the client stops it or the session
// TTL expires; every statement inside it is one "sub query" addressed by an integer id.
// The object status codes and the "ok" information status are the SDK's own constants
// (Session.OBJECT_STATUS_*, Session.SubQueryInfo.kOKCode), so SDK polling loops
// terminate on the same values they do against the service.

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

const (
	mcqaStatusRunning    = 2
	mcqaStatusFailed     = 4
	mcqaStatusTerminated = 5
	mcqaStatusCancelled  = 6

	mcqaInfoOK     = "ok"
	mcqaInfoFailed = "Failed"

	// mcqaDefaultTaskName matches Session.DEFAULT_TASK_NAME in the Java SDK.
	mcqaDefaultTaskName = "console_sqlrt_task"

	// mcqaRetainedQueries bounds how many finished sub-query results a session keeps
	// readable. Results are polled right after submission, so retention beyond this is
	// only a memory question; older ones are dropped FIFO.
	mcqaRetainedQueries = 16
)

// mcqaQuery is one sub query submitted into a session.
type mcqaQuery struct {
	Query    string
	Status   int
	Result   string
	Warnings string
	ID       int
	Created  time.Time
	// Data is the typed result behind the CSV text of the information channel. The
	// instance-tunnel read serves it as records, so a consumer gets the same values and
	// the same types through either path. It is retained for the same window as the CSV
	// text — the mcqaRetainedQueries entries dropped by mcqaSession.drop, and counted by
	// bytes into the instance budget.
	Data engine.Result
}

// mcqaSession is the state behind a SQLRT instance. Fields are guarded by Server.mu;
// statement execution runs outside that lock (see runSubQuery).
type mcqaSession struct {
	name string
	// selectOnly is the session's promise that nothing but a whole select runs here.
	// The service defaults it to true; a client that wants DDL or DML inside the session
	// says so with odps.sql.session.select.only.
	selectOnly bool
	live       bool
	nextID     int
	lastID     int
	queries    map[int]*mcqaQuery
	order      []int
	lastActive time.Time
}

func newMCQASession(name string, selectOnly bool) *mcqaSession {
	return &mcqaSession{name: name, selectOnly: selectOnly, live: true, queries: map[int]*mcqaQuery{}, lastActive: time.Now()}
}

// expired reports whether the session has been idle past the configured TTL. Idle, not
// age, is the right clock: an interactive session is expected to outlive one statement.
func (m *mcqaSession) expired(ttl time.Duration, now time.Time) bool {
	return m != nil && now.Sub(m.lastActive) > ttl
}

// mcqaProperty is one Entry of the task's Config block; the SDK puts the JSON-encoded
// session settings into the property named "settings".
type mcqaProperty struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

func mcqaTaskSettings(props []mcqaProperty) string {
	for _, p := range props {
		if p.Name == "settings" {
			return p.Value
		}
	}
	return ""
}

// drop discards the oldest retained sub-query result to keep the session bounded.
func (m *mcqaSession) drop() {
	for len(m.order) >= mcqaRetainedQueries {
		oldest := m.order[0]
		m.order = m.order[1:]
		delete(m.queries, oldest)
	}
}

// bytes is the retained payload of the session's sub-query results.
func (m *mcqaSession) bytes() int64 {
	if m == nil {
		return 0
	}
	var total int64
	for _, q := range m.queries {
		total += q.Data.Bytes + int64(len(q.Result)+len(q.Query))
	}
	return total
}

// subQueryResponse is what the SDK deserializes into Session.SubQueryResponse. An empty
// or unparseable body is read by the SDK as "the session went away", so every answer is
// valid JSON carrying a status.
type subQueryResponse struct {
	Status   int    `json:"status"`
	Result   string `json:"result"`
	Warnings string `json:"warnings,omitempty"`
	SubQuery int    `json:"subQueryId"`
}

// informationResult is what the SDK deserializes into Instance.SetInformationResult.
type informationResult struct {
	Result string `json:"result"`
	Status string `json:"status"`
}

// instanceTaskInfo is what Instance#setInformation marshals: the model's root element is
// `Instance` (its @Root name), carrying Key and Value. The decoder stays root-agnostic so
// the same handler also answers a client that sends the documented InstanceTaskInfo root.
type instanceTaskInfo struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type subQueryRequest struct {
	Query    string            `json:"query"`
	Settings map[string]string `json:"settings"`
}

// mcqaResponse renders a sub query for the key the client is polling.
func mcqaResponse(q *mcqaQuery) subQueryResponse {
	if q == nil {
		return subQueryResponse{Status: mcqaStatusFailed, Result: "SubQuery not found"}
	}
	return subQueryResponse{Status: q.Status, Result: q.Result, Warnings: q.Warnings, SubQuery: q.ID}
}

// mcqaCreate registers a session instance from an Instance/Job/Tasks/SQLRT POST and
// answers 201. It executes nothing: a session is a container, statements arrive later
// through the information KV.
func (s *Server) mcqaCreate(w http.ResponseWriter, r *http.Request, p, sc, taskName, settings string) {
	if taskName == "" {
		taskName = mcqaDefaultTaskName
	}
	name, _ := mcqaSetting(settings, "odps.sql.session.name")
	i := &instance{
		Project: p, Schema: sc, ID: id(), Name: taskName, TaskType: "SQLRT",
		Status: "Running", Created: time.Now(),
		MCQA: newMCQASession(name, !sessionRunsNonSelect(mcqaSettingsMap(settings))),
	}
	if !s.reserveInstance(i) {
		fail(w, r, 429, "ResourceLimit", fmt.Errorf("instance limit"))
		return
	}
	w.Header().Set("Location", "/projects/"+url.PathEscape(p)+"/instances/"+i.ID)
	w.WriteHeader(201)
}

// mcqaSetting pulls one key out of a task's `settings` JSON property.
func mcqaSetting(settings, key string) (string, bool) {
	if strings.TrimSpace(settings) == "" {
		return "", false
	}
	var m map[string]any
	if e := json.Unmarshal([]byte(settings), &m); e != nil {
		return "", false
	}
	v, ok := m[key]
	if !ok {
		return "", false
	}
	return fmt.Sprint(v), true
}

// mcqaStop terminates a session on the client's PUT Instance/Status=Terminated.
func (s *Server) mcqaStop(w http.ResponseWriter, r *http.Request, i *instance) {
	s.mu.Lock()
	if i.MCQA != nil {
		i.MCQA.live = false
	}
	i.Status = "Terminated"
	s.mu.Unlock()
	w.WriteHeader(204)
}

// mcqaInfo implements the GET/PUT `?info` half of the session protocol.
func (s *Server) mcqaInfo(w http.ResponseWriter, r *http.Request, i *instance) {
	switch r.Method {
	case http.MethodGet:
		s.mcqaGetInfo(w, r, i)
	case http.MethodPut:
		s.mcqaSetInfo(w, r, i)
	default:
		fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("unsupported instance information method"))
	}
}

func (s *Server) mcqaGetInfo(w http.ResponseWriter, r *http.Request, i *instance) {
	key := r.URL.Query().Get("key")
	s.mu.Lock()
	m := i.MCQA
	s.mu.Unlock()
	if m == nil {
		fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("instance information requires a SQLRT session task"))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m.lastActive = time.Now()
	if !m.live {
		jsonResponse(w, 200, subQueryResponse{Status: mcqaStatusTerminated, Result: "Session " + i.ID + " is stopped"})
		return
	}
	switch {
	case key == "status", key == "wait_attach_success":
		message := "Session " + i.ID + " started"
		if m.name != "" {
			message = "Session " + m.name + " started"
		}
		jsonResponse(w, 200, subQueryResponse{Status: mcqaStatusRunning, Result: message})
	case key == "progress":
		// Every information key answers inside the SubQueryResponse envelope, which is
		// how the service delivers SessionProgress (see Session.isStarted). One worker
		// is "launched" as soon as the session exists, since the emulator has no
		// distributed runtime to warm up.
		progress, _ := json.Marshal(map[string]any{
			"totalWorkerCount": 1, "launchedWorkerCount": 1, "launchedPercentage": 100,
		})
		jsonResponse(w, 200, subQueryResponse{Status: mcqaStatusRunning, Result: string(progress)})
	case key == "result":
		jsonResponse(w, 200, mcqaResponse(m.queries[m.lastID]))
	case strings.HasPrefix(key, "result_"):
		id, e := strconv.Atoi(strings.TrimPrefix(key, "result_"))
		if e != nil {
			jsonResponse(w, 200, subQueryResponse{Status: mcqaStatusFailed, Result: "Invalid sub query id: " + key})
			return
		}
		jsonResponse(w, 200, mcqaResponse(m.queries[id]))
	case strings.HasPrefix(key, "sqlstats"):
		// Statement statistics are not simulated; an empty terminated payload keeps
		// Session.getQueryStats() returning "" instead of a parse failure.
		jsonResponse(w, 200, subQueryResponse{Status: mcqaStatusTerminated})
	default:
		jsonResponse(w, 200, subQueryResponse{Status: mcqaStatusFailed, Result: "UnsupportedInformationKey: " + key})
	}
}

func (s *Server) mcqaSetInfo(w http.ResponseWriter, r *http.Request, i *instance) {
	var req instanceTaskInfo
	if e := xml.NewDecoder(r.Body).Decode(&req); e != nil || req.Key == "" {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("instance information requires Key and Value"))
		return
	}
	s.mu.Lock()
	m := i.MCQA
	if m == nil {
		s.mu.Unlock()
		s.informationReply(w, mcqaInfoFailed, "Instance information requires a SQLRT session task")
		return
	}
	m.lastActive = time.Now()
	if !m.live {
		s.mu.Unlock()
		s.informationReply(w, mcqaInfoFailed, "Session "+i.ID+" is stopped")
		return
	}
	switch req.Key {
	case "query":
		var sub subQueryRequest
		if e := json.Unmarshal([]byte(req.Value), &sub); e != nil || strings.TrimSpace(sub.Query) == "" {
			s.mu.Unlock()
			s.informationReply(w, mcqaInfoFailed, "Invalid sub query payload")
			return
		}
		if m.selectOnly && !sessionRunsNonSelect(sub.Settings) && !isSelectStatement(sub.Query) {
			// Refuse before running anything. A client that only learns the statement
			// was not a select when it comes to fetch the result reruns it offline —
			// and an INSERT that already ran in the session then applies twice. queryId
			// -1 with status ok is how the service reports a sub query it declined to
			// start, and the message is the pair FallbackPolicy matches on ("ODPS-185"),
			// which is what turns the refusal into an offline rerun rather than an error.
			s.mu.Unlock()
			payload, _ := json.Marshal(map[string]any{"queryId": -1, "status": mcqaInfoOK, "result": mcqaSelectOnlyRefusal})
			s.informationReply(w, mcqaInfoOK, string(payload))
			return
		}
		m.nextID++
		id := m.nextID
		m.drop()
		m.queries[id] = &mcqaQuery{Query: sub.Query, Status: mcqaStatusRunning, ID: id, Created: time.Now()}
		m.order = append(m.order, id)
		m.lastID = id
		s.mu.Unlock()
		// Execution runs with the session lock released: the client polls result_<id>
		// meanwhile and observes the running status, as it does against the service.
		s.runSubQuery(r.Context(), i, id, sub.Query)
		payload, _ := json.Marshal(map[string]any{"queryId": id, "status": mcqaInfoOK})
		s.informationReply(w, mcqaInfoOK, string(payload))
	case "cancel":
		id, e := strconv.Atoi(strings.TrimSpace(req.Value))
		if e != nil {
			s.mu.Unlock()
			s.informationReply(w, mcqaInfoFailed, "Invalid sub query id")
			return
		}
		q := m.queries[id]
		switch {
		case q == nil:
			s.mu.Unlock()
			s.informationReply(w, mcqaInfoFailed, "SubQuery not found")
		case q.Status != mcqaStatusRunning:
			s.mu.Unlock()
			// Statements execute eagerly, so a cancel that arrives after the result
			// exists is reported as a no-op instead of rewriting a delivered answer.
			s.informationReply(w, mcqaInfoFailed, fmt.Sprintf("SubQuery %d already terminated", id))
		default:
			q.Status = mcqaStatusCancelled
			q.Result = "Sub query cancelled"
			s.mu.Unlock()
			s.informationReply(w, mcqaInfoOK, "")
		}
	default:
		s.mu.Unlock()
		s.informationReply(w, mcqaInfoFailed, "UnsupportedInformationKey: "+req.Key)
	}
}

func (s *Server) informationReply(w http.ResponseWriter, status, result string) {
	jsonResponse(w, 200, informationResult{Status: status, Result: result})
}

// runSubQuery executes one statement of a session and stores its answer.
func (s *Server) runSubQuery(ctx context.Context, i *instance, id int, query string) {
	res, e := s.Engine.Execute(ctx, i.Project, i.Schema, query)
	s.mu.Lock()
	defer s.mu.Unlock()
	if i.MCQA == nil {
		return
	}
	q := i.MCQA.queries[id]
	if q == nil {
		return // dropped by the retention window while running
	}
	switch {
	case e != nil:
		q.Status = mcqaStatusFailed
		q.Result = e.Error()
	case ctx.Err() != nil:
		q.Status = mcqaStatusCancelled
		q.Result = "Sub query cancelled"
	default:
		q.Status = mcqaStatusTerminated
		q.Data = res
		q.Result = sessionResultCSV(res)
	}
}

// sessionResultCSV renders a result the way the SQLRT information channel does: the
// first line is the column-name header, because CSVRecordParser#parse reads that line
// to build the schema. Values are formatted like the offline instance result, and every
// row of the result is rendered — the tunnel read of the same sub query serves the same
// rows, so neither path can look truncated relative to the other.
func sessionResultCSV(res engine.Result) string {
	header := make([]string, len(res.Columns))
	for n, c := range res.Columns {
		header[n] = c.Name
	}
	var b strings.Builder
	w := csv.NewWriter(&b)
	w.UseCRLF = true
	if len(header) > 0 {
		w.Write(header)
	}
	for _, row := range res.Rows {
		line := make([]string, len(row))
		for n, v := range row {
			if v != nil {
				line[n] = fmt.Sprint(v)
			} else {
				line[n] = "\\N"
			}
		}
		w.Write(line)
	}
	w.Flush()
	return b.String()
}

// mcqaIdle reports whether a session instance has been idle past the configured TTL.
// Server.mu must already be held by the caller: the instance sweep in reserveInstance
// runs under that lock, and re-entering it there would self-deadlock (Server.mu is not
// reentrant). See TestMCQASessionDoesNotBlockOfflineInstances.
func (s *Server) mcqaIdle(i *instance) bool {
	return i.MCQA != nil && i.MCQA.expired(s.cfg.SessionTTL, time.Now())
}

// mcqaInstanceXML answers the plain instance GET for a session. The task stays RUNNING
// while the session is alive — Session.checkTaskStatus turns any other status into a
// session error, which is exactly what a stopped or unknown session should do.
func (s *Server) mcqaInstanceXML(w http.ResponseWriter, i *instance) {
	s.mu.Lock()
	live := i.MCQA.live
	name := i.MCQA.name
	start := i.Created.UTC().Format(http.TimeFormat)
	end := i.MCQA.lastActive.UTC().Format(http.TimeFormat)
	s.mu.Unlock()
	// Instance.reload parses x-odps-start-time unconditionally and x-odps-end-time
	// right after it, so an instance document without them fails before the body is
	// even read. A live session has not ended; the last activity is the closest
	// honest value and is what the SDK shows as the end time.
	w.Header().Set("x-odps-start-time", start)
	w.Header().Set("x-odps-end-time", end)
	w.Header().Set("x-odps-owner", "emulator")
	status, taskStatus := "Terminated", "Success"
	if live {
		status, taskStatus = "Running", "Running"
	}
	body := "<Instance><Status>" + status + "</Status><Tasks><Task Type=\"SQLRT\"><Name>" + esc(i.Name) +
		"</Name><Status>" + taskStatus + "</Status><StartTime>" + start + "</StartTime>"
	if name != "" {
		body += "<Properties><Property><Name>odps.sql.session.name</Name><Value>" + esc(name) + "</Value></Property></Properties>"
	}
	xmlResponse(w, body+"<Result></Result></Task></Tasks></Instance>")
}
