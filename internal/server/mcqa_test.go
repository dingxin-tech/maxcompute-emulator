package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const sessionTask = "console_sqlrt_task"

// mcqaCall sends a request and keeps the headers: the session id only comes back in
// Location, exactly like the service, so the tests have to read it the way the SDK does.
func mcqaCall(t *testing.T, h *httptest.Server, method, path string, body []byte) (int, http.Header, []byte) {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, _ := http.NewRequest(method, h.URL+path, r)
	res, e := h.Client().Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer res.Body.Close()
	b, e := io.ReadAll(res.Body)
	if e != nil {
		t.Fatal(e)
	}
	return res.StatusCode, res.Header, b
}

// createSession posts the Instance/Job/Tasks/SQLRT body the Java SDK sends for
// Session.create / SQLExecutor(INTERACTIVE) and returns the new instance id.
func createSession(t *testing.T, h *httptest.Server, settings string) string {
	t.Helper()
	body := `<Instance><Job><Tasks><SQLRT><Name>` + sessionTask + `</Name>`
	if settings != "" {
		body += `<Config><Property><Name>settings</Name><Value>` + settings + `</Value></Property></Config>`
	}
	body += `</SQLRT></Tasks></Job></Instance>`
	code, header, b := mcqaCall(t, h, "POST", "/projects/p/instances", []byte(body))
	if code != 201 {
		t.Fatalf("session create code=%d body=%s", code, b)
	}
	loc := header.Get("Location")
	if !strings.HasPrefix(loc, "/projects/p/instances/") {
		t.Fatalf("session create Location=%q", loc)
	}
	return strings.TrimPrefix(loc, "/projects/p/instances/")
}

func infoPath(id, taskName, key string) string {
	if key == "" {
		return "/projects/p/instances/" + id + "?info&taskname=" + taskName
	}
	return "/projects/p/instances/" + id + "?info&taskname=" + taskName + "&key=" + key
}

func getInfo(t *testing.T, h *httptest.Server, path string) subQueryResponse {
	t.Helper()
	code, _, b := mcqaCall(t, h, "GET", path, nil)
	if code != 200 {
		t.Fatalf("GET %s code=%d body=%s", path, code, b)
	}
	var r subQueryResponse
	if e := json.Unmarshal(b, &r); e != nil {
		t.Fatalf("GET %s is not SubQueryResponse JSON: %v / %s", path, e, b)
	}
	if r.Status == 0 {
		t.Fatalf("GET %s has no status field: %s", path, b)
	}
	return r
}

// submitStatement is what Session.runSubQuery does: one setInformation("query", json).
func submitStatement(t *testing.T, h *httptest.Server, id, query string) (int, informationResult) {
	t.Helper()
	payload, e := json.Marshal(map[string]string{"query": query})
	if e != nil {
		t.Fatal(e)
	}
	body := "<Instance><Key>query</Key><Value>" + string(payload) + "</Value></Instance>"
	code, _, b := mcqaCall(t, h, "PUT", infoPath(id, sessionTask, ""), []byte(body))
	if code != 200 {
		t.Fatalf("setInformation code=%d body=%s", code, b)
	}
	var ir informationResult
	if e := json.Unmarshal(b, &ir); e != nil {
		t.Fatalf("setInformation body not JSON: %v / %s", e, b)
	}
	return parseQueryID(t, ir), ir
}

func parseQueryID(t *testing.T, ir informationResult) int {
	t.Helper()
	var sub struct {
		QueryID int `json:"queryId"`
	}
	if e := json.Unmarshal([]byte(ir.Result), &sub); e != nil {
		t.Fatalf("sub query payload: %v / %s", e, ir.Result)
	}
	return sub.QueryID
}

func TestMCQASessionSubQueryLoop(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, `{"odps.sql.session.name":"demo"}`)
	// A live session must report its task as RUNNING: Session.checkTaskStatus turns
	// anything else into a session error.
	_, _, b := request(t, h, "GET", "/projects/p/instances/"+id, "")
	if !strings.Contains(string(b), "<Status>Running</Status>") || !strings.Contains(string(b), `Type="SQLRT"`) {
		t.Fatalf("live session instance status: %s", b)
	}
	if r := getInfo(t, h, infoPath(id, sessionTask, "status")); r.Status != mcqaStatusRunning || !strings.Contains(r.Result, "demo") {
		t.Fatalf("status key: %+v", r)
	}
	// Submitting a statement returns the sub-query id inside SetInformationResult.
	subQueryID, ir := submitStatement(t, h, id, "select id, s from t order by id limit 2")
	if ir.Status != mcqaInfoOK || subQueryID != 1 {
		t.Fatalf("setInformation status=%q result=%q id=%d", ir.Status, ir.Result, subQueryID)
	}
	// The answer is CSV with a header line, which is what CSVRecordParser#parse needs
	// to build a schema.
	r := getInfo(t, h, infoPath(id, sessionTask, fmt.Sprintf("result_%d", subQueryID)))
	if r.Status != mcqaStatusTerminated {
		t.Fatalf("terminated status expected for a completed sub query: %+v", r)
	}
	lines := strings.Split(strings.TrimRight(r.Result, "\r\n"), "\r\n")
	if len(lines) != 3 || lines[0] != "id,s" || !strings.HasPrefix(lines[1], "0,valuevalue") {
		t.Fatalf("result csv shape: %q", r.Result)
	}
	// The deprecated Session.run() path polls the bare "result" key instead.
	if bare := getInfo(t, h, infoPath(id, sessionTask, "result")); bare.SubQuery != subQueryID || bare.Status != mcqaStatusTerminated {
		t.Fatalf("bare result key: %+v", bare)
	}
	// A second statement gets the next id, and progress is answerable as JSON.
	if id2, _ := submitStatement(t, h, id, "select 1"); id2 != 2 {
		t.Fatalf("second sub query id=%d", id2)
	}
	if p := getInfo(t, h, infoPath(id, sessionTask, "progress")); p.Status != mcqaStatusRunning || !strings.Contains(p.Result, "launchedWorkerCount") {
		t.Fatalf("progress on a live session: %+v", p)
	}
}

func TestMCQASessionFailedStatementIsNotATransportError(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	if _, ir := submitStatement(t, h, id, "select * from missing_table"); ir.Status != mcqaInfoOK {
		t.Fatalf("accepted-but-failed statement must report ok at the KV layer: %+v", ir)
	}
	r := getInfo(t, h, infoPath(id, sessionTask, "result_1"))
	if r.Status != mcqaStatusFailed || r.Result == "" {
		t.Fatalf("failed statement: %+v", r)
	}
	// The session survives a bad statement: interactive clients keep using it.
	if got := getInfo(t, h, infoPath(id, sessionTask, "status")); got.Status != mcqaStatusRunning {
		t.Fatalf("session died after a failed statement: %+v", got)
	}
}

func TestMCQASessionUnknownKeysAreStructured(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	if r := getInfo(t, h, infoPath(id, sessionTask, "bogus")); r.Status != mcqaStatusFailed || !strings.Contains(r.Result, "UnsupportedInformationKey") {
		t.Fatalf("unknown info key: %+v", r)
	}
	if r := getInfo(t, h, infoPath(id, sessionTask, "result_notanumber")); r.Status != mcqaStatusFailed {
		t.Fatalf("non-numeric result key: %+v", r)
	}
	// A never-submitted id reports the same message the SDK special-cases.
	if r := getInfo(t, h, infoPath(id, sessionTask, "result_99")); r.Status != mcqaStatusFailed || !strings.Contains(r.Result, "SubQuery not found") {
		t.Fatalf("missing sub query: %+v", r)
	}
	code, rb := sendBody(t, h, "PUT", infoPath(id, sessionTask, ""), []byte("<Instance><Key>bogus</Key><Value>1</Value></Instance>"), nil)
	if code != 200 || !strings.Contains(string(rb), "UnsupportedInformationKey") {
		t.Fatalf("unknown setInformation key: code=%d body=%s", code, rb)
	}
}

func TestMCQASessionStop(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	if code, _, b := mcqaCall(t, h, "PUT", "/projects/p/instances/"+id, []byte("<Instance><Status>Terminated</Status></Instance>")); code != 204 {
		t.Fatalf("stop: code=%d body=%s", code, b)
	}
	if r := getInfo(t, h, infoPath(id, sessionTask, "status")); r.Status != mcqaStatusTerminated {
		t.Fatalf("status after stop: %+v", r)
	}
	code, _, rb := mcqaCall(t, h, "PUT", infoPath(id, sessionTask, ""), []byte(`<Instance><Key>query</Key><Value>{"query":"select 1"}</Value></Instance>`))
	if code != 200 || strings.Contains(string(rb), `"status":"ok"`) || !strings.Contains(string(rb), "stopped") {
		t.Fatalf("statement on a stopped session: code=%d body=%s", code, rb)
	}
	_, _, b := request(t, h, "GET", "/projects/p/instances/"+id, "")
	if !strings.Contains(string(b), "<Status>Terminated</Status>") {
		t.Fatalf("instance status after stop: %s", b)
	}
}

func TestMCQAInformationRejectsOneShotInstances(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	code, header, b := mcqaCall(t, h, "POST", "/projects/p/instances",
		[]byte(`<Instance><Job><Tasks><SQL><Name>oneshot</Name><Query>select 1</Query></SQL></Tasks></Job></Instance>`))
	if code != 201 {
		t.Fatalf("one-shot create: code=%d body=%s", code, b)
	}
	id := strings.TrimPrefix(header.Get("Location"), "/projects/p/instances/")
	// Asking the information KV of a one-shot SQL instance is a protocol error, not an
	// empty answer: the emulator must not pretend the instance is a session.
	if code, _, b = mcqaCall(t, h, "GET", infoPath(id, "oneshot", "status"), nil); code != 400 || !strings.Contains(string(b), "UnsupportedOperation") {
		t.Fatalf("information key on a one-shot instance: code=%d body=%s", code, b)
	}
	// Only sessions may be stopped; a finished one-shot instance has no lifecycle to stop.
	if code, _, b = mcqaCall(t, h, "PUT", "/projects/p/instances/"+id, []byte("<Instance><Status>Terminated</Status></Instance>")); code != 400 {
		t.Fatalf("stop a one-shot instance: code=%d body=%s", code, b)
	}
	// Unknown instances answer the information KV like the service does: not found.
	if code, _, b = mcqaCall(t, h, "GET", infoPath("missing", sessionTask, "status"), nil); code != 404 {
		t.Fatalf("missing instance information: code=%d body=%s", code, b)
	}
}

func TestMCQASessionCancelIsHonest(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	if _, ir := submitStatement(t, h, id, "select 1"); ir.Status != mcqaInfoOK {
		t.Fatalf("submit: %+v", ir)
	}
	// The emulator runs a statement before answering the submission, so cancel can only
	// arrive after the result exists — reporting that as success would be a lie.
	code, _, b := mcqaCall(t, h, "PUT", infoPath(id, sessionTask, ""), []byte("<Instance><Key>cancel</Key><Value>1</Value></Instance>"))
	if code != 200 || strings.Contains(string(b), `"status":"ok"`) || !strings.Contains(string(b), "already terminated") {
		t.Fatalf("cancel a delivered sub query: code=%d body=%s", code, b)
	}
	code, _, b = mcqaCall(t, h, "PUT", infoPath(id, sessionTask, ""), []byte("<Instance><Key>cancel</Key><Value>99</Value></Instance>"))
	if code != 200 || !strings.Contains(string(b), "SubQuery not found") {
		t.Fatalf("cancel an unknown sub query: code=%d body=%s", code, b)
	}
}

// TestMCQASessionDoesNotBlockOfflineInstances is the regression guard for a
// self-deadlock: the instance sweep in reserveInstance runs under Server.mu, so any
// expiry check it makes must not take that lock again. With a live session in the map,
// every later instance creation goes through that sweep — one hung offline statement
// would stall the whole emulator, including /readyz.
func TestMCQASessionDoesNotBlockOfflineInstances(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, `{"odps.sql.session.name":"demo"}`)
	if _, ir := submitStatement(t, h, id, "select 1"); ir.Status != mcqaInfoOK {
		t.Fatalf("submit: %+v", ir)
	}
	for n := 0; n < 3; n++ {
		done := make(chan int, 1)
		go func() {
			code, _, _ := mcqaCall(t, h, "POST", "/projects/p/instances",
				[]byte(`<Instance><Job><Tasks><SQL><Name>offline</Name><Query>select 1</Query></SQL></Tasks></Job></Instance>`))
			done <- code
		}()
		select {
		case code := <-done:
			if code != 201 {
				t.Fatalf("offline instance with a live session present: code=%d", code)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a live session wedged instance creation")
		}
	}
	if r := getInfo(t, h, infoPath(id, sessionTask, "status")); r.Status != mcqaStatusRunning {
		t.Fatalf("session must survive alongside offline instances: %+v", r)
	}
}

// TestMCQAInformationBodyRoots covers the wire shape the SDK actually sends:
// Instance.InstanceTaskInfoModel is marshalled with @Root(name = "Instance"), so a
// decoder that only accepts an InstanceTaskInfo root rejects every real submission.
func TestMCQAInformationBodyRoots(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	for _, root := range []string{"Instance", "InstanceTaskInfo"} {
		id := createSession(t, h, "")
		body := "<" + root + "><Key>query</Key><Value>" + `{"query":"select 1"}` + "</Value></" + root + ">"
		code, _, b := mcqaCall(t, h, "PUT", infoPath(id, sessionTask, ""), []byte(body))
		if code != 200 || !strings.Contains(string(b), `"status":"ok"`) {
			t.Fatalf("root %s: code=%d body=%s", root, code, b)
		}
		if r := getInfo(t, h, infoPath(id, sessionTask, "result_1")); r.Status != mcqaStatusTerminated {
			t.Fatalf("root %s sub query answer: %+v", root, r)
		}
	}
	// A body without a Key is a client error, not an empty answer.
	id := createSession(t, h, "")
	if code, _, _ := mcqaCall(t, h, "PUT", infoPath(id, sessionTask, ""), []byte("<Instance><Value>x</Value></Instance>")); code != 400 {
		t.Fatalf("keyless information body: code=%d", code)
	}
}

// TestMCQASessionNamesDoNotAttach documents the boundary the emulator does not cross:
// a session name is metadata, not a rendezvous. Real named-session sharing needs
// server-side attach semantics the public SDK does not describe, so two creates with the
// same name are two sessions rather than one shared one, and nothing silently reuses an
// instance the client never asked for.
func TestMCQASessionNamesDoNotAttach(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	settings := `{"odps.sql.session.name":"shared"}`
	first := createSession(t, h, settings)
	second := createSession(t, h, settings)
	if first == second {
		t.Fatal("named creates must not collapse into one instance")
	}
	for _, id := range []string{first, second} {
		if r := getInfo(t, h, infoPath(id, sessionTask, "status")); r.Status != mcqaStatusRunning || !strings.Contains(r.Result, "shared") {
			t.Fatalf("session %s status: %+v", id, r)
		}
	}
	// Statements stay inside the session that submitted them.
	if _, ir := submitStatement(t, h, first, "select 1"); ir.Status != mcqaInfoOK {
		t.Fatalf("submit to first: %+v", ir)
	}
	if r := getInfo(t, h, infoPath(second, sessionTask, "result_1")); r.Status != mcqaStatusFailed || !strings.Contains(r.Result, "SubQuery not found") {
		t.Fatalf("the other session must not see the statement: %+v", r)
	}
}

func TestMCQASessionIdleExpiry(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: 10 * time.Millisecond})
	id := createSession(t, h, "")
	time.Sleep(30 * time.Millisecond)
	code, _, b := request(t, h, "GET", "/projects/p/instances/"+id, "")
	if code != 404 || !strings.Contains(string(b), "NoSuchInstance") {
		t.Fatalf("idle session must expire: code=%d body=%s", code, b)
	}
}
