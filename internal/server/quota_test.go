package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestNamedQuotaRoutesStorageAndTunnelSessions(t *testing.T) {
	s, h := fixture(t, Config{Quotas: []string{"batch", "interactive"}})
	if _, err := s.Engine.Execute(context.Background(), "p", "default", "create table quota_rows (id bigint)"); err != nil {
		t.Fatal(err)
	}
	host := strings.TrimPrefix(h.URL, "http://")
	for _, param := range []string{"quota", "quotaName"} {
		code, headers, body := request(t, h, "GET", "/api/projects/p/tunnel?"+param+"=batch", "")
		if code != 200 || string(body) != host+"/__quota/batch" || headers.Get("x-odps-tunnel-quota-name") != "batch" {
			t.Fatalf("discovery with %s returned %d %v %q", param, code, headers, body)
		}
	}
	for _, tc := range []struct {
		path  string
		code  int
		error string
	}{
		{"/api/projects/p/tunnel?quota=unknown", 404, "QuotaNotExist"},
		{"/api/projects/p/tunnel?quota=batch&quotaName=interactive", 400, "InvalidParameter"},
		{"/__quota/batch/api/projects/p/tunnel?quotaName=interactive", 400, "InvalidParameter"},
	} {
		code, _, body := request(t, h, "GET", tc.path, "")
		if code != tc.code || !strings.Contains(string(body), tc.error) {
			t.Fatalf("%s returned %d %s", tc.path, code, body)
		}
	}

	storage := "/api/storage/v3?Target=projects.p.schemas.default.tables.quota_rows&Action="
	code, headers, body := request(t, h, "POST", "/__quota/batch"+storage+"TableCreateReadSession", "")
	var snapshot struct{ SessionID string }
	if code != 200 || headers.Get("x-odps-tunnel-quota-name") != "batch" ||
		json.Unmarshal(body, &snapshot) != nil || snapshot.SessionID == "" {
		t.Fatalf("storage create returned %d %s", code, body)
	}
	load := storage + "TableGetReadSession&SessionId=" + url.QueryEscape(snapshot.SessionID)
	for _, route := range []string{"", "/__quota/interactive", "/__quota/batch"} {
		code, _, body := request(t, h, "POST", route+load, "")
		if route == "/__quota/batch" && code != 200 || route != "/__quota/batch" &&
			(code != http.StatusNotFound || !strings.Contains(string(body), "NoSuchSession")) {
			t.Fatalf("storage reload on %q returned %d %s", route, code, body)
		}
	}

	table := "/projects/p/tables/quota_rows"
	code, headers, body = request(t, h, "POST", "/__quota/batch"+table+"?downloads", "")
	var tunnel struct{ DownloadID string }
	if code != 200 || headers.Get("x-odps-tunnel-quota-name") != "batch" ||
		json.Unmarshal(body, &tunnel) != nil || tunnel.DownloadID == "" {
		t.Fatalf("tunnel create returned %d %s", code, body)
	}
	path := table + "?downloadid=" + url.QueryEscape(tunnel.DownloadID)
	code, _, body = request(t, h, "GET", "/__quota/interactive"+path, "")
	if code != http.StatusNotFound || !strings.Contains(string(body), "NoSuchDownload") {
		t.Fatalf("cross-quota reload returned %d %s", code, body)
	}
	code, headers, body = request(t, h, "GET", "/__quota/batch"+path, "")
	if code != 200 || headers.Get("x-odps-tunnel-quota-name") != "batch" {
		t.Fatalf("tunnel reload returned %d %s", code, body)
	}
}
