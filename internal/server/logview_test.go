package server

import (
	"encoding/xml"
	"net/http/httptest"
	"strings"
	"testing"
)

// bearerResponse mirrors the model SecurityManager#generateAuthorizationToken parses:
// root element Authorization, child Result holding the token. Parsing the answer with a
// copy of that shape is the point of the test — a response the SDK cannot unmarshal is a
// failed statement no matter what the status code says.
type bearerResponse struct {
	XMLName xml.Name `xml:"Authorization"`
	Result  string   `xml:"Result"`
}

func postBody(t *testing.T, s *Server, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("POST", path, strings.NewReader(body)))
	return w
}

// instancePolicy is the body LogView#generatePolicy builds for a statement's logview URL.
func instancePolicy(project, instance string) string {
	return `{"expires_in_hours": 24, "policy": {"Statement": [{"Action": ["odps:Read"],` +
		` "Effect": "Allow", "Resource": "acs:odps:*:projects/` + project + `/instances/` + instance +
		`"}], "Version": "1"}}`
}

func TestLogViewHostIsPlainText(t *testing.T) {
	s := New(mustEngine(t), Config{Project: "p"})
	w := s.do(t, "GET", "/logview/host")
	if w.Code != 200 {
		t.Fatalf("host lookup: code=%d body=%s", w.Code, w.Body)
	}
	if w.Body.String() != logViewHost {
		t.Fatalf("host=%q want %q", w.Body.String(), logViewHost)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content type=%q, the SDK reads the body as one plain string", ct)
	}
	if w := s.do(t, "POST", "/logview/host"); w.Code != 405 {
		t.Fatalf("POST /logview/host: code=%d want 405", w.Code)
	}
}

func TestBearerTokenIsSignedPerRequest(t *testing.T) {
	s := New(mustEngine(t), Config{Project: "p"})
	w := postBody(t, s, "/projects/p/authorization?sign_bearer_token", instancePolicy("p", "i1"))
	if w.Code != 200 {
		t.Fatalf("sign token: code=%d body=%s", w.Code, w.Body)
	}
	var out bearerResponse
	if e := xml.Unmarshal(w.Body.Bytes(), &out); e != nil {
		t.Fatalf("the SDK model cannot parse %s: %v", w.Body, e)
	}
	if !strings.HasPrefix(out.Result, bearerTokenPrefix) || out.Result == bearerTokenPrefix {
		t.Fatalf("token=%q want a %q-prefixed placeholder", out.Result, bearerTokenPrefix)
	}
	// The console and policy templates spell Resource as an array; both forms must sign.
	if w := postBody(t, s, "/projects/p/authorization?sign_bearer_token",
		`{"expires_in_hours": 1, "policy": {"Statement": [{"Action": ["odps:Read"], "Effect": "Allow", "Resource": ["acs:odps:*:projects/p/instances/i1"]}], "Version": "1"}}`); w.Code != 200 {
		t.Fatalf("array-shaped policy: code=%d body=%s", w.Code, w.Body)
	}
	// Two statements must not share one token, or a captured logview URL would outlive the
	// instance it was minted for and the log line would name the wrong request.
	again := postBody(t, s, "/projects/p/authorization?sign_bearer_token", instancePolicy("p", "i2"))
	var second bearerResponse
	if e := xml.Unmarshal(again.Body.Bytes(), &second); e != nil {
		t.Fatalf("second response unparseable: %v", e)
	}
	if second.Result == out.Result {
		t.Fatalf("two statements got the same token %q", out.Result)
	}
}

func TestBearerTokenRejectsRequestsThatNameNothing(t *testing.T) {
	s := New(mustEngine(t), Config{Project: "p"})
	cases := []struct {
		name, path, method, body string
		want                     int
		contains                 string
	}{
		{"not a policy", "/projects/p/authorization?sign_bearer_token", "POST", "create table t(a int)", 400, "InvalidParameter"},
		{"no resource", "/projects/p/authorization?sign_bearer_token", "POST", `{"policy":{"Statement":[],"Version":"1"}}`, 400, "names no resource"},
		{"no token type", "/projects/p/authorization", "POST", instancePolicy("p", "i1"), 400, "sign_bearer_token"},
		{"wrong method", "/projects/p/authorization?sign_bearer_token", "GET", "", 405, "POST"},
		{"unknown project", "/projects/nope/authorization?sign_bearer_token", "POST", instancePolicy("nope", "i1"), 404, "NoSuchProject"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := postBody(t, s, c.path, c.body)
			if c.method != "POST" {
				w = s.do(t, c.method, c.path)
			}
			if w.Code != c.want {
				t.Fatalf("code=%d want %d body=%s", w.Code, c.want, w.Body)
			}
			if !strings.Contains(w.Body.String(), c.contains) {
				t.Fatalf("body=%s want it to say %q", w.Body, c.contains)
			}
		})
	}
}

// TestMCQAConnectionRefusalNamesTheGap pins the client-visible text. A driver configured
// for MaxQA keeps this message and appends it to every later SQLException, so "unsupported
// endpoint" would come back looking like an emulator bug on an unrelated statement.
func TestMCQAConnectionRefusalNamesTheGap(t *testing.T) {
	s := New(mustEngine(t), Config{Project: "p"})
	w := s.do(t, "GET", "/connection/mcqa?project=p&quota=mcqa_quota")
	if w.Code != 404 {
		t.Fatalf("code=%d want 404 (a refusal is what selects the SQLRT path), body=%s", w.Code, w.Body)
	}
	body := w.Body.String()
	for _, want := range []string{"UnsupportedOperation", "MCQA v2", "interactiveMode=mcqa"} {
		if !strings.Contains(body, want) {
			t.Fatalf("body=%s missing %q", body, want)
		}
	}
	if w := s.do(t, "GET", "/connection/mcqa?project=nope"); w.Code != 404 || !strings.Contains(w.Body.String(), "NoSuchProject") {
		t.Fatalf("unknown project: code=%d body=%s", w.Code, w.Body)
	}
}
