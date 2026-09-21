package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

type traceKey struct{}
type requestTrace struct {
	Action string `json:"action"`
	// Plane separates the Tunnel data plane ("tunnel") from the REST metadata
	// plane ("rest": resources, registration/functions). Object is the metadata
	// collection the request addresses and is empty on the Tunnel plane.
	Plane        string `json:"plane,omitempty"`
	Object       string `json:"object,omitempty"`
	DownloadHash string `json:"download_id_hash"`
	Project      string `json:"project"`
	Table        string `json:"table"`
	Partition    bool   `json:"partition_present"`
	Start        uint64 `json:"start"`
	Count        uint64 `json:"count"`
	Columns      int    `json:"columns_count"`
	Format       string `json:"format"`
	Compression  string `json:"compression"`
	Status       int    `json:"status"`
	ErrorCode    string `json:"error_code,omitempty"`
	Elapsed      int64  `json:"elapsed_ms"`
	Quota        string `json:"quota"`
	fault        *FaultEffect
}

func trace(r *http.Request) *requestTrace {
	v, _ := r.Context().Value(traceKey{}).(*requestTrace)
	return v
}
func (s *Server) hashSession(v string) string {
	if v == "" {
		return ""
	}
	h := hmac.New(sha256.New, s.logKey)
	h.Write([]byte(v))
	return hex.EncodeToString(h.Sum(nil)[:12])
}
func (s *Server) requestTrace(r *http.Request) *requestTrace {
	q := r.URL.Query()
	t := &requestTrace{Status: 200, Format: "protobuf", Compression: "identity", Partition: q.Has("partition"), Quota: q.Get("quotaName")}
	path := strings.TrimPrefix(r.URL.Path, "/api/")
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "projects" {
			t.Project = parts[i+1]
		}
		if parts[i] == "tables" {
			t.Table = parts[i+1]
		}
	}
	target := strings.Split(q.Get("Target"), ".")
	if (path == "storage/v2" || path == "storage/v3") && len(target) >= 2 && target[0] == "projects" {
		t.Project = target[1]
		if len(target) == 6 && target[4] == "tables" {
			t.Table = target[5]
		}
	}
	resource := parts
	if len(resource) >= 4 && resource[0] == "projects" {
		resource = resource[2:]
		if len(resource) >= 4 && resource[0] == "schemas" {
			resource = resource[2:]
		}
	}
	tunnelResource := len(resource) == 2 && (resource[0] == "tables" || resource[0] == "instances")
	if tunnelResource && !q.Has("uploads") && !q.Has("uploadid") {
		if q.Has("downloads") && r.Method == "POST" {
			t.Action = "create"
		} else if q.Has("downloadid") {
			if r.Method == "GET" {
				t.Action = "reload"
				if q.Has("data") {
					t.Action = "read"
				}
			}
			if r.Method == "POST" {
				t.Action = "complete"
			}
		}
	}
	// Metadata-plane requests are addressed below the project node, with an
	// optional schema segment: projects/P[/schemas/S]/resources[/NAME] and
	// projects/P[/schemas/S]/registration/functions[/NAME]. The action is the
	// request verb, which is what a client retries, not the resulting state:
	// POST is create, PUT is update, GET/HEAD is read, DELETE is delete.
	if len(parts) >= 3 && parts[0] == "projects" {
		sub := parts[2:]
		if len(sub) >= 2 && sub[0] == "schemas" {
			sub = sub[2:]
		}
		switch {
		case len(sub) >= 1 && sub[0] == "resources":
			t.Plane, t.Object = "rest", "resources"
		case len(sub) >= 2 && sub[0] == "registration" && sub[1] == "functions":
			t.Plane, t.Object = "rest", "functions"
		}
		if t.Object != "" {
			switch r.Method {
			case "GET", "HEAD":
				t.Plane, t.Action = "rest", "read"
			case "POST":
				t.Plane, t.Action = "rest", "create"
			case "PUT":
				t.Plane, t.Action = "rest", "update"
			case "DELETE":
				t.Plane, t.Action = "rest", "delete"
			default:
				t.Plane, t.Object = "", ""
			}
		}
	}
	if t.Action != "" && t.Plane == "" {
		t.Plane = "tunnel"
	}
	t.DownloadHash = s.hashSession(q.Get("downloadid"))
	if q.Has("arrow") {
		t.Format = "arrow"
	}
	if v := q.Get("columns"); v != "" {
		t.Columns = len(strings.Split(v, ","))
	}
	a := strings.Split(strings.Trim(q.Get("rowrange"), "()"), ",")
	if len(a) == 2 {
		t.Start, _ = strconv.ParseUint(a[0], 10, 64)
		t.Count, _ = strconv.ParseUint(a[1], 10, 64)
	}
	if t.Quota == "" {
		t.Quota = q.Get("quota_name")
	}
	if t.Quota == "" {
		t.Quota = "default"
	}
	s.mu.Lock()
	if v := s.sessions[q.Get("downloadid")]; v != nil {
		t.Quota = v.Quota
	}
	s.mu.Unlock()
	if t.Quota == "" {
		t.Quota = "default"
	}
	return t
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(n int) {
	if w.status == 0 {
		w.status = n
		w.ResponseWriter.WriteHeader(n)
	}
}
func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Fault rules are process-local, bounded, and consumed atomically in rule-ID order.
// Attempts count matching requests since PUT, including requests beyond the hit budget.
type FaultMatch struct {
	Action  string `json:"action"`
	Plane   string `json:"plane,omitempty"`
	Object  string `json:"object,omitempty"`
	Project string `json:"project"`
	Table   string `json:"table"`
	Format  string `json:"format"`
	Quota   string `json:"quota"`
	Attempt int    `json:"attempt"`
}
type FaultEffect struct {
	Type    string `json:"type"`
	Rows    int    `json:"rows,omitempty"`
	Bytes   int    `json:"bytes,omitempty"`
	Status  int    `json:"status,omitempty"`
	Code    string `json:"code,omitempty"`
	DelayMS int    `json:"delay_ms,omitempty"`
}
type FaultRule struct {
	Match      FaultMatch  `json:"match"`
	Effect     FaultEffect `json:"effect"`
	Times      int         `json:"times"`
	TTLSeconds int         `json:"ttl_seconds"`
	Expires    time.Time   `json:"expires_at"`
	Hits       int         `json:"hits"`
	Attempts   int         `json:"attempts"`
}

func (s *Server) faultsAPI(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.TestMode {
		http.NotFound(w, r)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/__test/faults")
	key = strings.TrimPrefix(key, "/")
	if strings.Contains(key, "/") || len(key) > 128 {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid rule id"))
		return
	}
	var rule FaultRule
	if r.Method == "PUT" {
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
		d.DisallowUnknownFields()
		if key == "" || d.Decode(&rule) != nil || d.Decode(new(any)) != io.EOF {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid fault rule"))
			return
		}
		if err := validateFault(&rule); err != nil {
			fail(w, r, 400, "InvalidParameter", err)
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, v := range s.faults {
		if now.After(v.Expires) {
			delete(s.faults, k)
		}
	}
	switch r.Method {
	case "DELETE":
		if key == "" {
			clear(s.faults)
		} else {
			delete(s.faults, key)
		}
		w.WriteHeader(204)
	case "GET":
		if v := s.faults[key]; v != nil {
			jsonResponse(w, 200, v)
		} else {
			http.NotFound(w, r)
		}
	case "PUT":
		v := rule
		if _, ok := s.faults[key]; !ok && len(s.faults) >= 128 {
			fail(w, r, 429, "ResourceLimit", fmt.Errorf("fault rule limit"))
			return
		}
		v.Expires = now.Add(time.Duration(v.TTLSeconds) * time.Second)
		v.Hits = 0
		v.Attempts = 0
		s.faults[key] = &v
		jsonResponse(w, 200, v)
	default:
		w.WriteHeader(405)
	}
}
func validateFault(v *FaultRule) error {
	if v.Effect.Status != 0 && (v.Effect.Status < 400 || v.Effect.Status > 599) {
		return fmt.Errorf("error status must be 400..599")
	}
	if len(v.Effect.Code) > 128 {
		return fmt.Errorf("error code too long")
	}
	if v.Times < 1 || v.Times > 10000 || v.Match.Attempt < 0 {
		return fmt.Errorf("times must be 1..10000 and attempt nonnegative")
	}
	if v.TTLSeconds == 0 {
		v.TTLSeconds = 60
	}
	if v.TTLSeconds < 1 || v.TTLSeconds > 3600 {
		return fmt.Errorf("ttl_seconds must be 1..3600")
	}
	if v.Effect.Rows < 0 || v.Effect.Rows > 100000 || v.Effect.Bytes < 0 || v.Effect.Bytes > 64<<20 || v.Effect.DelayMS < 0 || v.Effect.DelayMS > 60000 {
		return fmt.Errorf("effect exceeds test limits")
	}
	if v.Match.Plane == "rest" {
		// The metadata plane has no streams, sessions, formats or quotas to
		// corrupt, so only the two effects a client retry loop actually reacts
		// to are offered: a structured HTTP failure and a slow response.
		switch v.Match.Action {
		case "create", "read", "update", "delete":
		default:
			return fmt.Errorf("explicit metadata-plane action required")
		}
		switch v.Match.Object {
		case "resources", "functions":
		default:
			return fmt.Errorf("metadata-plane fault requires object resources or functions")
		}
		if v.Match.Table != "" || v.Match.Format != "" || v.Match.Quota != "" {
			return fmt.Errorf("table, format and quota are Tunnel match dimensions")
		}
		switch v.Effect.Type {
		case "http_error":
			if v.Effect.Status < 400 || v.Effect.Status > 599 {
				return fmt.Errorf("http_error requires status 400..599")
			}
		case "delay":
		default:
			return fmt.Errorf("metadata-plane faults support http_error and delay")
		}
		return nil
	}
	if v.Match.Object != "" {
		return fmt.Errorf("object match requires plane rest")
	}
	switch v.Match.Action {
	case "create", "reload", "read", "complete":
	default:
		return fmt.Errorf("explicit Tunnel action required")
	}
	if v.Match.Format != "" && v.Match.Format != "arrow" && v.Match.Format != "protobuf" {
		return fmt.Errorf("invalid format")
	}
	switch v.Effect.Type {
	case "http_error":
		if v.Effect.Status < 400 || v.Effect.Status > 599 {
			return fmt.Errorf("http_error requires status 400..599")
		}
	case "complete_error":
		if v.Match.Action != "complete" {
			return fmt.Errorf("complete_error requires complete")
		}
	case "delay", "expire_session":
	case "disconnect_after_bytes", "disconnect_after_rows", "early_eof", "crc_mismatch", "malformed_protobuf", "malformed_arrow", "empty_arrow_batch", "oversized_arrow_batch":
		if v.Match.Action != "read" {
			return fmt.Errorf("stream fault requires read")
		}
		if strings.Contains(v.Effect.Type, "arrow") && v.Match.Format != "arrow" {
			return fmt.Errorf("Arrow fault requires format arrow")
		}
		if v.Effect.Type == "malformed_protobuf" && v.Match.Format != "protobuf" {
			return fmt.Errorf("protobuf fault requires format protobuf")
		}
	default:
		return fmt.Errorf("unknown effect")
	}
	return nil
}
func (s *Server) selectFault(t *requestTrace) *FaultEffect {
	if !s.cfg.TestMode || t.Action == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.faults))
	for k := range s.faults {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := s.faults[k]
		if time.Now().After(v.Expires) {
			delete(s.faults, k)
			continue
		}
		m := v.Match
		plane := t.Plane
		if plane == "" {
			plane = "tunnel"
		}
		if m.Plane == "" {
			m.Plane = "tunnel"
		}
		if m.Plane != plane || m.Object != t.Object || m.Action != t.Action || m.Project != "" && m.Project != t.Project || m.Table != "" && m.Table != t.Table || m.Format != "" && m.Format != t.Format || m.Quota != "" && m.Quota != t.Quota {
			continue
		}
		v.Attempts++
		if v.Hits >= v.Times || m.Attempt > 0 && v.Attempts < m.Attempt {
			continue
		}
		v.Hits++
		e := v.Effect
		return &e
	}
	return nil
}

// beforeProjectRequest applies the Tunnel quota contract and then the selected
// test fault. It runs for every project-scoped request, so the quota block is
// gated on the Tunnel plane: a metadata-plane request neither names a quota nor
// carries the tunnel quota header.
func (s *Server) beforeProjectRequest(w http.ResponseWriter, r *http.Request) bool {
	t := trace(r)
	if t == nil {
		return true
	}
	if t.Action != "" && t.Plane == "tunnel" || strings.HasSuffix(r.URL.Path, "/tunnel") {
		found := t.Quota == "default"
		for _, q := range s.cfg.Quotas {
			if q == t.Quota {
				found = true
			}
		}
		if !found {
			fail(w, r, 404, "QuotaNotExist", fmt.Errorf("specified quota not found"))
			return false
		}
		w.Header().Set("x-odps-tunnel-quota-name", t.Quota)
	}
	t.fault = s.selectFault(t)
	e := t.fault
	if e == nil {
		return true
	}
	switch e.Type {
	case "delay":
		timer := time.NewTimer(time.Duration(e.DelayMS) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			fail(w, r, 408, "RequestTimeout", fmt.Errorf("request cancelled or timed out"))
			return false
		case <-timer.C:
		}
	case "http_error", "complete_error":
		status := e.Status
		if status == 0 {
			status = 500
		}
		code := e.Code
		if code == "" {
			code = "InternalError"
		}
		if status == 429 {
			w.Header().Set("Retry-After", "1")
			if e.Code == "" {
				code = "FlowExceeded"
			}
		}
		fail(w, r, status, code, fmt.Errorf("injected test failure"))
		return false
	case "expire_session":
		s.mu.Lock()
		delete(s.sessions, r.URL.Query().Get("downloadid"))
		s.mu.Unlock()
		fail(w, r, 404, "NoSuchDownload", fmt.Errorf("session expired"))
		return false
	}
	return true
}
func faultRows(r *http.Request, res engine.Result) engine.Result {
	t := trace(r)
	if t == nil || t.fault == nil {
		return res
	}
	e := t.fault
	switch e.Type {
	case "early_eof", "disconnect_after_rows":
		res.Rows = res.Rows[:min(e.Rows, len(res.Rows))]
	case "oversized_arrow_batch":
		if len(res.Rows) > 0 {
			rows := append([][]any(nil), res.Rows...)
			for len(rows) <= int(t.Count) && len(rows) <= 100000 {
				rows = append(rows, res.Rows[0])
			}
			res.Rows = rows
		}
	}
	return res
}
func withTrace(r *http.Request, t *requestTrace) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), traceKey{}, t))
}
