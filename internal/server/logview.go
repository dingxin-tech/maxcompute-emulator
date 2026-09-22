package server

// The endpoints a client calls on the way to running a statement, none of which returns
// table data. They live together because missing any of them shows up as a failed
// statement whose error text has nothing to do with the SQL the user ran:
//
//	GET  /logview/host                                   -> plain-text logview host
//	POST /projects/{p}/authorization?sign_bearer_token   -> <Authorization><Result>token
//	GET  /connection/mcqa?project={p}&[quota={q}]        -> refused, MCQA v2 is not simulated
//
// The Java SDK builds a logview URL for every statement it submits
// (SQLExecutorImpl#runInOffline -> LogView#generateLogView ->
// SecurityManager#generateAuthorizationToken), and the JDBC driver turns a failure there
// into a SQLException. Before this file the driver could not run even "create table"
// against the emulator in offline mode: the instance was created and executed, then the
// token request 404'd and the statement was reported as failed.

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

// logViewHost is the host the SDK itself falls back to when the frontend does not answer,
// so replying costs a constant and saves a request per statement.
const logViewHost = "http://logview.odps.aliyun.com"

// bearerTokenPrefix marks the strings this process signed. They are placeholders, not
// credentials: the emulator authenticates requests with access-key or STS fixtures (see
// authDescription in auth.go) and never accepts a bearer token as proof of identity. The
// prefix is on purpose, so a token captured in a test log cannot be mistaken for a real
// MaxCompute bearer token.
const bearerTokenPrefix = "emulator-bearer-"

// logView answers GET /logview/host. LogView#getLogviewHost reads the body as plain text.
func (s *Server) logView(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		fail(w, r, 405, "InvalidParameter", fmt.Errorf("the logview host endpoint only accepts GET"))
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	io.WriteString(w, logViewHost)
}

// policyList accepts both spellings a policy document uses for a list of strings: the bare
// string LogView#generatePolicy writes for Resource, and the array form policy templates
// and the console write. Rejecting the first would refuse the only request the SDK makes.
type policyList []string

func (l *policyList) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '[' {
		var many []string
		if e := json.Unmarshal(b, &many); e != nil {
			return e
		}
		*l = many
		return nil
	}
	var one string
	if e := json.Unmarshal(b, &one); e != nil {
		return e
	}
	*l = []string{one}
	return nil
}

// bearerPolicy is the JSON body SecurityManager#generateAuthorizationToken sends: an
// expiry plus a statement listing what the token may read.
type bearerPolicy struct {
	ExpiresInHours int `json:"expires_in_hours"`
	Policy         struct {
		Version   string `json:"Version"`
		Statement []struct {
			Effect   string     `json:"Effect"`
			Action   policyList `json:"Action"`
			Resource policyList `json:"Resource"`
		} `json:"Statement"`
	} `json:"policy"`
}

// resources names what the policy asked to read, for the log line.
func (b bearerPolicy) resources() string {
	out := ""
	for _, statement := range b.Policy.Statement {
		for _, resource := range statement.Resource {
			if out != "" {
				out += ","
			}
			out += resource
		}
	}
	return out
}

// signBearerToken answers POST /projects/{p}/authorization?sign_bearer_token. The response
// is the XML AuthorizationQueryResponse maps: root Authorization, element Result.
//
// The token grants nothing here, so the answer only has to be well formed and stable per
// request. The policy is still parsed, because a client that sends something unparseable
// is broken in a way worth reporting at the call that noticed it rather than in a logview
// URL nobody can open.
func (s *Server) signBearerToken(w http.ResponseWriter, r *http.Request, p string) {
	if !r.URL.Query().Has("sign_bearer_token") {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("the only supported authorization operation is ?sign_bearer_token"))
		return
	}
	if r.Method != http.MethodPost {
		fail(w, r, 405, "InvalidParameter", fmt.Errorf("signing a bearer token requires POST"))
		return
	}
	body, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if e != nil {
		fail(w, r, 400, "InvalidParameter", e)
		return
	}
	var policy bearerPolicy
	if e := json.Unmarshal(body, &policy); e != nil {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("the bearer token policy must be a JSON object: %v", e))
		return
	}
	if policy.resources() == "" {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("the bearer token policy names no resource"))
		return
	}
	slog.Info("bearer token", "project", p, "resources", policy.resources(), "expires_in_hours", policy.ExpiresInHours)
	xmlResponse(w, "<Authorization><Result>"+esc(bearerTokenPrefix+id())+"</Result></Authorization>")
}

// mcqaConnection answers the MCQA v2 (MaxQA) routing lookup that Quotas#getMaxQAConnInfo
// performs. The emulator runs SQLRT sessions only, and a driver decides between the two by
// whether this call succeeds, so the answer has to be a refusal. Refusing by name is what
// makes the difference visible: the JDBC driver keeps the failure text of this call and
// appends it to every later SQLException, so a generic 404 would come back attached to an
// unrelated statement error.
func (s *Server) mcqaConnection(w http.ResponseWriter, r *http.Request) {
	if project := r.URL.Query().Get("project"); project != "" && !s.checkProject(w, r, project) {
		return // checkProject already said which kind of unknown project this is
	}
	fail(w, r, 404, "UnsupportedOperation", fmt.Errorf("MCQA v2 (MaxQA) is not simulated by the emulator; run a SQLRT session with interactiveMode=mcqa, or submit the statement with interactiveMode=offline"))
}
