package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// restCall is the unsigned (permissive-auth) shape of a metadata-plane request.
func restCall(t *testing.T, h *httptest.Server, method, path string, headers map[string]string, body string) (int, http.Header, []byte) {
	t.Helper()
	r, _ := http.NewRequest(method, h.URL+path, strings.NewReader(body))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	resp, e := h.Client().Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b
}

func signedCall(t *testing.T, h *httptest.Server, method, path string, headers map[string]string, body, secret, cred string) (int, http.Header, []byte) {
	t.Helper()
	r, _ := http.NewRequest(method, h.URL+path, strings.NewReader(body))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	// Date and every x-odps- header are part of the canonical request, so the
	// signature has to be computed after the request is fully assembled.
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	r.Header.Set("Authorization", "ODPS "+cred+":"+authSignature(r, secret, cred))
	resp, e := h.Client().Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b
}

const functionBody = "<Function><Alias>word_count</Alias><ClassType>com.example.WC</ClassType><Resources><ResourceName>wc.py</ResourceName></Resources></Function>"

func TestRestPlaneFaultInjection(t *testing.T) {
	s, h := fixture(t, Config{TestMode: true})
	putFault(t, h, "create-500", FaultRule{Match: FaultMatch{Plane: "rest", Object: "resources", Action: "create", Project: "p"}, Effect: FaultEffect{Type: "http_error", Status: 500}, Times: 1})
	code, _, b := restCall(t, h, "POST", "/projects/p/resources", map[string]string{"x-odps-resource-name": "late.py", "x-odps-resource-type": "py"}, "print(2)")
	if code != 500 || !bytes.Contains(b, []byte("<Code>InternalError</Code>")) {
		t.Fatalf("injected create: %d %s", code, b)
	}
	// An injected failure rejects the request before the handler runs, so it
	// must not leave a partial object behind.
	if code, _, _ = restCall(t, h, "GET", "/projects/p/resources/late.py?meta", nil, ""); code != 404 {
		t.Fatalf("partial object after injected failure: %d", code)
	}
	if code, _, b = restCall(t, h, "POST", "/projects/p/resources", map[string]string{"x-odps-resource-name": "late.py", "x-odps-resource-type": "py"}, "print(2)"); code != 201 {
		t.Fatalf("retry after budget: %d %s", code, b)
	}
	var rule FaultRule
	if resp, e := h.Client().Get(h.URL + "/__test/faults/create-500"); e != nil {
		t.Fatal(e)
	} else {
		json.NewDecoder(resp.Body).Decode(&rule)
		resp.Body.Close()
	}
	// Attempts counts every matching request, including the one served after
	// the hit budget was spent; Hits is the number actually injected.
	if rule.Hits != 1 || rule.Attempts != 2 {
		t.Fatalf("rule accounting: %+v", rule)
	}
	// 429 on a metadata read keeps Retry-After, which is the signal the Java SDK
	// RestClient reacts to (4xx is otherwise not retried).
	putFault(t, h, "read-429", FaultRule{Match: FaultMatch{Plane: "rest", Object: "resources", Action: "read"}, Effect: FaultEffect{Type: "http_error", Status: 429}, Times: 1})
	code, hdr, b := restCall(t, h, "GET", "/projects/p/resources", nil, "")
	if code != 429 || hdr.Get("Retry-After") != "1" || !bytes.Contains(b, []byte("FlowExceeded")) {
		t.Fatalf("injected read: %d %v %s", code, hdr, b)
	}
	// A delay on the same plane exercises client-side timeout handling.
	putFault(t, h, "read-delay", FaultRule{Match: FaultMatch{Plane: "rest", Object: "functions", Action: "read"}, Effect: FaultEffect{Type: "delay", DelayMS: 40}, Times: 1})
	start := time.Now()
	if code, _, _ = restCall(t, h, "GET", "/projects/p/registration/functions", nil, ""); code != 200 || time.Since(start) < 30*time.Millisecond {
		t.Fatalf("delayed function list: %d in %s", code, time.Since(start))
	}
	// Object and action must not leak across: a functions rule cannot touch a
	// resources request, and vice versa.
	if code, _, b = restCall(t, h, "POST", "/projects/p/resources", map[string]string{"x-odps-resource-name": "wc.py", "x-odps-resource-type": "py"}, "print(1)"); code != 201 {
		t.Fatalf("seed dependency resource: %d %s", code, b)
	}
	if code, _, b = restCall(t, h, "POST", "/projects/p/registration/functions", map[string]string{"Content-Type": "application/xml"}, functionBody); code != 201 {
		t.Fatalf("seed function: %d %s", code, b)
	}
	putFault(t, h, "fn-delete", FaultRule{Match: FaultMatch{Plane: "rest", Object: "functions", Action: "delete"}, Effect: FaultEffect{Type: "http_error", Status: 503, Code: "ServiceUnavailable"}, Times: 1})
	if code, _, b = restCall(t, h, "DELETE", "/projects/p/resources/late.py", nil, ""); code != 200 {
		t.Fatalf("resources delete hit a functions rule: %d %s", code, b)
	}
	if code, _, b = restCall(t, h, "DELETE", "/projects/p/registration/functions/word_count", nil, ""); code != 503 || !bytes.Contains(b, []byte("ServiceUnavailable")) {
		t.Fatalf("function delete not injected: %d %s", code, b)
	}
	if code, _, _ = restCall(t, h, "DELETE", "/projects/p/registration/functions/word_count", nil, ""); code != 200 {
		t.Fatalf("function delete retry: %d", code)
	}
	_ = s
}

func TestRestFaultRulesAreBounded(t *testing.T) {
	for _, tc := range []struct {
		name string
		rule FaultRule
		ok   bool
	}{
		{"http_error", FaultRule{Times: 1, Match: FaultMatch{Plane: "rest", Object: "resources", Action: "create"}, Effect: FaultEffect{Type: "http_error", Status: 503}}, true},
		{"delay", FaultRule{Times: 1, Match: FaultMatch{Plane: "rest", Object: "functions", Action: "read"}, Effect: FaultEffect{Type: "delay", DelayMS: 10}}, true},
		{"missing action", FaultRule{Times: 1, Match: FaultMatch{Plane: "rest", Object: "resources"}, Effect: FaultEffect{Type: "http_error", Status: 503}}, false},
		{"missing object", FaultRule{Times: 1, Match: FaultMatch{Plane: "rest", Action: "read"}, Effect: FaultEffect{Type: "http_error", Status: 503}}, false},
		{"unknown object", FaultRule{Times: 1, Match: FaultMatch{Plane: "rest", Object: "volumes", Action: "read"}, Effect: FaultEffect{Type: "http_error", Status: 503}}, false},
		{"tunnel verb", FaultRule{Times: 1, Match: FaultMatch{Plane: "rest", Object: "resources", Action: "reload"}, Effect: FaultEffect{Type: "http_error", Status: 503}}, false},
		{"stream effect", FaultRule{Times: 1, Match: FaultMatch{Plane: "rest", Object: "resources", Action: "read"}, Effect: FaultEffect{Type: "early_eof", Rows: 1}}, false},
		{"table dimension", FaultRule{Times: 1, Match: FaultMatch{Plane: "rest", Object: "resources", Action: "read", Table: "t"}, Effect: FaultEffect{Type: "http_error", Status: 503}}, false},
		{"format dimension", FaultRule{Times: 1, Match: FaultMatch{Plane: "rest", Object: "resources", Action: "read", Format: "arrow"}, Effect: FaultEffect{Type: "http_error", Status: 503}}, false},
		{"quota dimension", FaultRule{Times: 1, Match: FaultMatch{Plane: "rest", Object: "resources", Action: "read", Quota: "q"}, Effect: FaultEffect{Type: "http_error", Status: 503}}, false},
		{"unknown plane", FaultRule{Times: 1, Match: FaultMatch{Plane: "mcqa", Object: "resources", Action: "read"}, Effect: FaultEffect{Type: "http_error", Status: 503}}, false},
		{"object without plane", FaultRule{Times: 1, Match: FaultMatch{Object: "resources", Action: "read"}, Effect: FaultEffect{Type: "http_error", Status: 503}}, false},
		{"tunnel rule unchanged", FaultRule{Times: 1, Match: FaultMatch{Action: "read", Format: "protobuf"}, Effect: FaultEffect{Type: "malformed_protobuf"}}, true},
	} {
		err := validateFault(&tc.rule)
		if tc.ok && err != nil {
			t.Fatalf("%s rejected: %v", tc.name, err)
		}
		if !tc.ok && err == nil {
			t.Fatalf("%s accepted", tc.name)
		}
	}
	_, h := fixture(t, Config{TestMode: true})
	b, _ := json.Marshal(FaultRule{Times: 1, Match: FaultMatch{Plane: "rest", Object: "resources", Action: "read"}, Effect: FaultEffect{Type: "crc_mismatch"}})
	r, _ := http.NewRequest("PUT", h.URL+"/__test/faults/bad", bytes.NewReader(b))
	resp, e := h.Client().Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	if body, _ := io.ReadAll(resp.Body); resp.StatusCode != 400 || !bytes.Contains(body, []byte("InvalidParameter")) {
		t.Fatalf("bad rule over HTTP: %d %s", resp.StatusCode, body)
	}
}

// TestStrictAuthMetadataPlane pins the ACL shape of the resources/functions
// plane: the metadata verbs are writes even when the Tunnel plane treats a POST
// as a read-session create, and project-wide objects need a project-wide grant.
func TestStrictAuthMetadataPlane(t *testing.T) {
	s, h := fixture(t, Config{})
	s.cfg.AuthMode = "strict"
	s.cfg.Credentials = map[string]Credential{
		"reader":  {Secret: "reader-secret", Read: []string{"p.*"}},
		"writer":  {Secret: "writer-secret", Read: []string{"p.*"}, Write: []string{"p.*"}},
		"narrow":  {Secret: "narrow-secret", Read: []string{"p.t"}},
		"foreign": {Secret: "foreign-secret", Read: []string{"p.*"}, Write: []string{"other.*"}},
	}
	res := map[string]string{"x-odps-resource-name": "wc.py", "x-odps-resource-type": "py"}
	if code, _, b := signedCall(t, h, "GET", "/projects/p/resources", nil, "", "reader-secret", "reader"); code != 200 {
		t.Fatalf("reader list: %d %s", code, b)
	}
	if code, _, b := signedCall(t, h, "POST", "/projects/p/resources", res, "print(1)", "reader-secret", "reader"); code != 403 || !bytes.Contains(b, []byte("NoPermission")) {
		t.Fatalf("reader create must be a write: %d %s", code, b)
	}
	if code, _, _ := signedCall(t, h, "GET", "/projects/p/resources/wc.py?meta", nil, "", "reader-secret", "reader"); code != 404 {
		t.Fatalf("denied create left state behind: %d", code)
	}
	if code, _, b := signedCall(t, h, "DELETE", "/projects/p/resources/wc.py", nil, "", "reader-secret", "reader"); code != 403 {
		t.Fatalf("reader delete: %d %s", code, b)
	}
	if code, _, b := signedCall(t, h, "POST", "/projects/p/resources", res, "print(1)", "foreign-secret", "foreign"); code != 403 {
		t.Fatalf("write grant for another project: %d %s", code, b)
	}
	if code, _, b := signedCall(t, h, "GET", "/projects/p/resources", nil, "", "narrow-secret", "narrow"); code != 403 {
		t.Fatalf("table-scoped grant on project-wide listing: %d %s", code, b)
	}
	if code, _, b := signedCall(t, h, "POST", "/projects/p/resources", res, "print(1)", "writer-secret", "writer"); code != 201 {
		t.Fatalf("writer create: %d %s", code, b)
	}
	if code, _, b := signedCall(t, h, "POST", "/projects/p/registration/functions", map[string]string{"Content-Type": "application/xml"}, functionBody, "reader-secret", "reader"); code != 403 {
		t.Fatalf("reader register function: %d %s", code, b)
	}
	if code, _, b := signedCall(t, h, "POST", "/projects/p/registration/functions", map[string]string{"Content-Type": "application/xml"}, functionBody, "writer-secret", "writer"); code != 201 {
		t.Fatalf("writer register function: %d %s", code, b)
	}
	if code, _, b := signedCall(t, h, "PUT", "/projects/p/registration/functions/word_count", map[string]string{"Content-Type": "application/xml"}, functionBody, "reader-secret", "reader"); code != 403 {
		t.Fatalf("reader update function: %d %s", code, b)
	}
	// The verb is inside the signature: a GET signed request replayed as a POST
	// cannot borrow the read grant.
	r, _ := http.NewRequest("GET", h.URL+"/projects/p/resources/wc.py", nil)
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	spoofed := authSignature(r, "reader-secret", "reader")
	r2, _ := http.NewRequest("POST", h.URL+"/projects/p/resources/wc.py", strings.NewReader("overwrite"))
	r2.Header.Set("Date", r.Header.Get("Date"))
	r2.Header.Set("Authorization", "ODPS reader:"+spoofed)
	resp, e := h.Client().Do(r2)
	if e != nil {
		t.Fatal(e)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("replay with another verb: %d %s", resp.StatusCode, body)
	}
}

// TestTunnelQuotaContractStaysOffMetadataPlane guards the other direction: naming
// a quota is a Tunnel concept, so a metadata request must not gain quota
// validation or the tunnel quota header, and Tunnel must keep both.
func TestTunnelQuotaContractStaysOffMetadataPlane(t *testing.T) {
	_, h := fixture(t, Config{TestMode: true})
	code, hdr, b := restCall(t, h, "GET", "/projects/p/resources?quotaName=absent", nil, "")
	if code != 200 || hdr.Get("x-odps-tunnel-quota-name") != "" {
		t.Fatalf("metadata plane gained quota handling: %d %v %s", code, hdr, b)
	}
	if code, hdr, b = request(t, h, "POST", "/projects/p/tables/t?downloads&quotaName=absent", ""); code != 404 || !bytes.Contains(b, []byte("QuotaNotExist")) || hdr.Get("x-odps-tunnel-quota-name") != "" {
		t.Fatalf("tunnel quota validation lost: %d %v %s", code, hdr, b)
	}
	if code, hdr, b = request(t, h, "POST", "/projects/p/tables/t?downloads&quotaName=q", ""); code != 200 || hdr.Get("x-odps-tunnel-quota-name") != "q" {
		t.Fatalf("tunnel quota header lost: %d %v %s", code, hdr, b)
	}
}

func TestAccessLogSeparatesPlanes(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(old)
	_, h := fixture(t, Config{})
	if code, _, b := restCall(t, h, "POST", "/projects/p/resources", map[string]string{"x-odps-resource-name": "wc.py", "x-odps-resource-type": "py"}, "print(1)"); code != 201 {
		t.Fatalf("create: %d %s", code, b)
	}
	request(t, h, "POST", "/projects/p/tables/t?downloads", "")
	h.Close()
	markers := []string{`"msg":"rest","request_id":"`, `"action":"create","object":"resources","project":"p"`, `"msg":"tunnel"`}
	for _, m := range markers {
		if !strings.Contains(logs.String(), m) {
			t.Fatalf("missing %s in\n%s", m, logs.String())
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if strings.Contains(line, `"msg":"rest"`) && strings.Contains(line, "wc.py") {
			t.Fatalf("resource name leaked into the access log: %s", line)
		}
	}
}
