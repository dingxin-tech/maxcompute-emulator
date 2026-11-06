package captureproxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestProxyForwardsSignedRequestAndRecordsSelectedHeaders(t *testing.T) {
	const (
		requestURI    = "/projects/a%2Fb/instances?part=a%2Bb&part=c;d&token=query-secret"
		authorization = "ODPS sample-ak:sample-signature"
		body          = "<Job>request-body</Job>"
	)
	trace := []string{"first", "second"}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.RequestURI != "/api"+requestURI {
			t.Errorf("unexpected upstream request %s %s", r.Method, r.RequestURI)
		}
		if !reflect.DeepEqual(r.Header.Values("Tracestate"), trace) {
			t.Errorf("upstream tracestate = %v", r.Header.Values("Tracestate"))
		}
		if r.Header.Get("Authorization") != authorization || r.Header.Get("Cookie") != "session=cookie-secret" {
			t.Error("credentials did not reach the fixed upstream")
		}
		data, err := io.ReadAll(r.Body)
		if err != nil || string(data) != body {
			t.Errorf("body = %q, err = %v", data, err)
		}
		w.Header().Set("X-Odps-Request-Id", "upstream-request")
		w.Header().Set("Location", "/instances/one")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "upstream response")
	}))
	defer upstream.Close()

	var output bytes.Buffer
	proxy, err := New(upstream.URL+"/api", []string{"tracestate", "traceparent"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, requestURI, strings.NewReader(body))
	request.Header.Set("Authorization", authorization)
	request.Header.Set("Cookie", "session=cookie-secret")
	request.Header["Tracestate"] = trace
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusCreated || response.Body.String() != "upstream response" ||
		response.Header().Get("Location") != "/instances/one" {
		t.Fatalf("unexpected response %d %q", response.Code, response.Body.String())
	}

	log := output.String()
	for _, forbidden := range []string{authorization, "query-secret", "cookie-secret", body, "upstream response"} {
		if strings.Contains(log, forbidden) {
			t.Errorf("capture exposed %q", forbidden)
		}
	}
	var got entry
	if err := json.NewDecoder(&output).Decode(&got); err != nil {
		t.Fatal(err)
	}
	wantHeaders := map[string][]string{"Tracestate": trace}
	if got.Method != http.MethodPost || got.Path != "/projects/a%2Fb/instances" ||
		got.Status != http.StatusCreated || got.RequestID != "upstream-request" ||
		!reflect.DeepEqual(got.RequestHeaders, wantHeaders) {
		t.Fatalf("unexpected capture %+v", got)
	}
}

func TestProxyRejectsCredentialCaptureAndInvalidUpstreams(t *testing.T) {
	for _, name := range []string{"authorization", "CoOkIe", "X-Odps-Security-Token",
		"X-Odps-App-Authentication", "Proxy-Authorization", "X-Api_Key", "Bad:Name"} {
		if _, err := New("http://localhost", []string{name}, io.Discard); err == nil {
			t.Errorf("allowed capture of %q", name)
		}
	}
	for _, upstream := range []string{"", "localhost:8080", "ftp://localhost", "http:///api",
		"http://user:password@localhost", "http://localhost/api?token=secret", "https://localhost/#secret"} {
		if _, err := New(upstream, nil, io.Discard); err == nil {
			t.Errorf("allowed invalid upstream %q", upstream)
		}
	}
}

func TestProxyRejectsForwardProxyRequests(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("rejected request reached the upstream")
	}))
	defer upstream.Close()
	proxy, err := New(upstream.URL, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method string
		target string
		status int
	}{
		{http.MethodConnect, "target.example:443", http.StatusMethodNotAllowed},
		{http.MethodGet, "http://target.example/projects", http.StatusBadRequest},
	} {
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, httptest.NewRequest(tc.method, tc.target, nil))
		if response.Code != tc.status {
			t.Errorf("%s returned %d, want %d", tc.method, response.Code, tc.status)
		}
	}
}

func TestProxyRecordsUpstreamFailure(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	upstream.Close()
	var output bytes.Buffer
	proxy, err := New(upstream.URL, nil, &output)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/projects", nil))
	var got entry
	if err := json.NewDecoder(&output).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusBadGateway || got.Status != http.StatusBadGateway ||
		got.Error != "upstream_transport" || got.RequestID != "" {
		t.Fatalf("unexpected failure response %d and capture %+v", response.Code, got)
	}
}
