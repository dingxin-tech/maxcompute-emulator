package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func putFault(t *testing.T, h *httptest.Server, key string, v FaultRule) {
	t.Helper()
	b, _ := json.Marshal(v)
	r, _ := http.NewRequest("PUT", h.URL+"/__test/faults/"+key, bytes.NewReader(b))
	resp, e := h.Client().Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
}
func TestTunnelJSONLifecycle(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(old)
	s, h := fixture(t, Config{})
	vals := make([]string, 10007)
	for i := range vals {
		vals[i] = fmt.Sprintf("(%d,'x')", i)
	}
	if _, err := s.Engine.Execute(context.Background(), "p", "default", "insert overwrite table t values "+strings.Join(vals, ",")); err != nil {
		t.Fatal(err)
	}
	_, _, body := request(t, h, "POST", "/projects/p/tables/t?downloads", "")
	var session map[string]any
	json.Unmarshal(body, &session)
	sid := session["DownloadID"].(string)
	base := "/projects/p/tables/t?downloadid=" + sid
	for i := 0; i < 16; i++ {
		request(t, h, "GET", base, "")
		n := 625
		if i == 15 {
			n = 632
		}
		request(t, h, "GET", base+fmt.Sprintf("&data&rowrange=(%d,%d)", i*625, n), "")
	}
	request(t, h, "POST", base, "")
	h.Close()
	counts := map[string]int{}
	hash := ""
	next := float64(0)
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil || m["msg"] != "tunnel" {
			continue
		}
		a := m["action"].(string)
		counts[a]++
		got := m["download_id_hash"].(string)
		if hash == "" {
			hash = got
		}
		if got != hash || got == "" {
			t.Fatal(m)
		}
		if a == "read" {
			if m["start"] != next {
				t.Fatal(m)
			}
			next += m["count"].(float64)
		}
		if a == "complete" && counts["read"] != 16 {
			t.Fatal(counts)
		}
	}
	if counts["create"] != 1 || counts["reload"] != 16 || counts["read"] != 16 || counts["complete"] != 1 || next != 10007 || strings.Contains(logs.String(), sid) {
		t.Fatal(counts, next, "session/log mismatch")
	}
}
func TestFaultEffectsAndRecovery(t *testing.T) {
	s, h := fixture(t, Config{TestMode: true})
	sid := sessionID(t, h)
	base := "/projects/p/tables/t?downloadid=" + sid
	for _, typ := range []string{"http_error", "disconnect_after_bytes", "disconnect_after_rows", "early_eof", "crc_mismatch", "malformed_protobuf", "malformed_arrow", "empty_arrow_batch", "oversized_arrow_batch", "delay", "complete_error", "expire_session"} {
		t.Run(typ, func(t *testing.T) {
			action, format, path := "read", "protobuf", base+"&data&rowrange=(0,8)"
			if strings.Contains(typ, "arrow") {
				format = "arrow"
				path += "&arrow"
			}
			if typ == "complete_error" {
				action = "complete"
				path = base
			}
			if typ == "expire_session" {
				action = "reload"
				path = base
			}
			effect := FaultEffect{Type: typ, Rows: 2, Bytes: 5}
			if typ == "http_error" {
				effect.Status = 429
				effect.Code = "FlowExceeded"
			}
			if typ == "delay" {
				effect.DelayMS = 20
			}
			putFault(t, h, "r", FaultRule{Match: FaultMatch{Action: action, Format: format, Quota: "q"}, Effect: effect, Times: 1})
			method := "GET"
			if action == "complete" {
				method = "POST"
			}
			before := time.Now()
			req, _ := http.NewRequest(method, h.URL+path, nil)
			req.Header.Set("Accept-Encoding", "identity")
			resp, e := h.Client().Do(req)

			if e != nil {
				t.Fatal(e)
			}
			b, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			switch typ {
			case "http_error":
				if resp.StatusCode != 429 || !bytes.Contains(b, []byte("FlowExceeded")) {
					t.Fatal(resp.StatusCode, string(b))
				}
			case "complete_error":
				if resp.StatusCode != 500 {
					t.Fatal(resp.StatusCode)
				}
			case "expire_session":
				if resp.StatusCode != 404 || !bytes.Contains(b, []byte("NoSuchDownload")) {
					t.Fatal(string(b))
				}
			case "disconnect_after_bytes", "disconnect_after_rows":
				if readErr == nil {
					t.Fatal("expected incomplete HTTP body")
				}
			case "delay":
				if time.Since(before) < 20*time.Millisecond {
					t.Fatal("delay not applied")
				}
			default:
				if resp.StatusCode != 200 || len(b) == 0 {
					t.Fatal(resp.StatusCode, string(b))
				}
			}
			s.mu.Lock()
			hits := s.faults["r"].Hits
			s.mu.Unlock()
			if hits != 1 {
				t.Fatal(hits)
			}
			if typ != "expire_session" {
				code, _, _ := request(t, h, "GET", base+"&data&rowrange=(0,8)", "")
				if code != 200 {
					t.Fatal(code)
				}
			}
		})
	}
}
func TestFaultBoundsExpiryCancellationAndDefaultOff(t *testing.T) {
	_, off := fixture(t, Config{})
	code, _, _ := request(t, off, "GET", "/__test/faults/x", "")
	if code != 404 {
		t.Fatal(code)
	}
	s, h := fixture(t, Config{TestMode: true})
	sid := sessionID(t, h)
	putFault(t, h, "delay", FaultRule{Match: FaultMatch{Action: "read", Attempt: 2}, Effect: FaultEffect{Type: "delay", DelayMS: 5000}, Times: 1})
	path := "/projects/p/tables/t?downloadid=" + sid + "&data&rowrange=(0,8)"
	request(t, h, "GET", path, "")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "GET", h.URL+path, nil)
	start := time.Now()
	resp, e := h.Client().Do(r)
	if resp != nil {
		resp.Body.Close()
	}
	if e == nil || time.Since(start) > time.Second {
		t.Fatal("cancel not honored", e)
	}
	s.mu.Lock()
	s.faults["delay"].Expires = time.Now().Add(-time.Second)
	s.mu.Unlock()
	request(t, h, "GET", path, "")
	code, _, _ = request(t, h, "GET", "/__test/faults/delay", "")
	if code != 404 {
		t.Fatal(code)
	}
	if validateFault(&FaultRule{Times: 1, Match: FaultMatch{Action: "read"}, Effect: FaultEffect{Type: "unknown"}}) == nil {
		t.Fatal("unknown effect accepted")
	}
}
func TestQuotaAndStrictAuth(t *testing.T) {
	s, h := fixture(t, Config{})
	for _, path := range []string{"/projects/p/tunnel?quotaName=absent", "/projects/p/tables/t?downloads&quotaName=absent"} {
		method := "GET"
		if strings.Contains(path, "?downloads") {
			method = "POST"
		}
		code, _, b := request(t, h, method, path, "")
		if code != 404 || !bytes.Contains(b, []byte("QuotaNotExist")) {
			t.Fatal(code, string(b))
		}
	}
	s.cfg.AuthMode = "strict"
	s.cfg.Credentials = map[string]Credential{"local": {Secret: "test-secret", Token: "test-token", Read: []string{"p.t"}}}
	for _, tc := range []struct {
		name, path, secret, token string
		date                      time.Time
		want                      int
	}{
		{"valid", "/projects/p/tables/t?downloads", "test-secret", "test-token", time.Now(), 200},
		{"signature", "/projects/p/tables/t?downloads", "bad", "test-token", time.Now(), 401},
		{"token", "/projects/p/tables/t?downloads", "test-secret", "bad", time.Now(), 401},
		{"expired", "/projects/p/tables/t?downloads", "test-secret", "test-token", time.Now().Add(-time.Hour), 401},
		{"permission", "/projects/p/tables/other?downloads", "test-secret", "test-token", time.Now(), 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := http.NewRequest("POST", h.URL+tc.path, nil)
			r.Header.Set("Date", tc.date.UTC().Format(http.TimeFormat))
			r.Header.Set("authorization-sts-token", tc.token)
			r.Header.Set("Authorization", "ODPS local:"+authSignature(r, tc.secret, "local"))
			resp, e := h.Client().Do(r)
			if e != nil {
				t.Fatal(e)
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tc.want {
				t.Fatal(resp.StatusCode, string(b))
			}
		})
	}
}

func TestStrictACLQueryCannotChangeResourceOrOperation(t *testing.T) {
	s, h := fixture(t, Config{})
	s.cfg.AuthMode = "strict"
	s.cfg.Credentials = map[string]Credential{"local": {Secret: "secret", Read: []string{"p.*"}, Write: []string{"other.*"}}}
	for _, path := range []string{
		"/projects/p/instances?downloads", "/projects/p/instances?Action=TableRead",
		"/projects/p/instances?Target=projects.other.schemas.default.tables.t",
		"/projects/p/tables/t?uploads&downloads", "/projects/p/tables/t?downloads",
	} {
		method := "POST"
		if path == "/projects/p/tables/t?downloads" {
			method = "DELETE"
		}
		r, _ := http.NewRequest(method, h.URL+path, strings.NewReader("unused"))
		r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
		r.Header.Set("Authorization", "ODPS local:"+authSignature(r, "secret", "local"))
		resp, err := h.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 403 || !bytes.Contains(b, []byte("NoPermission")) {
			t.Fatal(path, resp.StatusCode, string(b))
		}
	}
}

func TestCanonicalStorageAndRESTPrefixes(t *testing.T) {
	for _, tc := range []struct{ path, want string }{{"/api/projects/p/tables/t", "/projects/p/tables/t"}, {"/api/storage/v3?Action=TableRead", "/api/storage/v3?Action=TableRead"}} {
		r := httptest.NewRequest("POST", tc.path, nil)
		if !strings.HasSuffix(canonicalRequest(r), tc.want) {
			t.Fatal(canonicalRequest(r))
		}
	}
}
