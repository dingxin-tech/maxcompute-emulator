package server

import (
	"context"
	"strings"
	"testing"

	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
)

func TestRequestCapacityDefaultsAndOverrides(t *testing.T) {
	if got := cap(New(nil, Config{}).inflight); got != 256 {
		t.Fatalf("default capacity %d", got)
	}
	if got := cap(New(nil, Config{MaxConcurrentRequests: 1024}).inflight); got != 1024 {
		t.Fatalf("configured capacity %d", got)
	}
}

func TestRequestAdmission429DoesNotMutateOrInflateWriteMetrics(t *testing.T) {
	s, h := fixture(t, Config{MaxConcurrentRequests: 1, TestMode: true})
	s.inflight <- struct{}{}
	code, headers, body := request(t, h, "POST", "/projects/p/instances", "")
	if code != 429 || headers.Get("Retry-After") != "1" || !strings.Contains(string(body), "FlowExceeded") {
		t.Fatalf("wrong throttle contract %d %v %s", code, headers, body)
	}
	sqlCode, sqlBody := sendBody(t, h, "POST", "/projects/p/instances", []byte(`<Instance><Job><Tasks><SQL><Name>limited</Name><Query>insert into t values(9999,'must not commit')</Query></SQL></Tasks></Job></Instance>`), nil)
	if sqlCode != 429 {
		t.Fatalf("SQL not rejected: %d %s", sqlCode, sqlBody)
	}
	code, _, _ = request(t, h, "POST", "/projects/p/tables/t?uploads", "")
	if code != 429 || s.activeWrites.Load() != 0 || s.peakWrites.Load() != 0 || s.peakDataWrites.Load() != 0 {
		t.Fatalf("rejected request inflated metrics: %d", code)
	}
	<-s.inflight
	result, err := s.Engine.Execute(context.Background(), "p", "default", "select count(*) from t where id=9999")
	if err != nil || wire.Int(result.Rows[0][0]) != 0 {
		t.Fatalf("rejected request mutated data %v %v", result, err)
	}
	code, _, body = request(t, h, "GET", "/readyz", "")
	if code != 200 {
		t.Fatalf("capacity not reusable %d %s", code, body)
	}
	if len(s.inflight) != 0 {
		t.Fatal("admission slot leaked")
	}
}
