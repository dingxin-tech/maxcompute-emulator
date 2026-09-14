package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
)

func TestInstanceCapacityRejectsBeforeSQL(t *testing.T) {
	s, h := fixture(t, Config{})
	for n := 0; n < 10000; n++ {
		s.instances[fmt.Sprint(n)] = &instance{Created: time.Now()}
	}
	code, body := sendBody(t, h, "POST", "/projects/p/instances", []byte(`<Instance><Job><Tasks><SQL><Name>capacity</Name><Query>insert into t values(9999,'must not commit')</Query></SQL></Tasks></Job></Instance>`), nil)
	if code != 429 {
		t.Fatalf("expected admission rejection, got %d: %s", code, body)
	}
	result, err := s.Engine.Execute(context.Background(), "p", "default", "select count(*) from t where id=9999")
	if err != nil || len(result.Rows) != 1 || wire.Int(result.Rows[0][0]) != 0 {
		t.Fatalf("rejected SQL changed data: %v, %v", result.Rows, err)
	}
}

func TestInstanceBudgetEvictsOldestCompleted(t *testing.T) {
	s := New(nil, Config{})
	now := time.Now()
	// Estimated row bytes let this test exercise the budget without large allocations.
	s.instances["old"] = &instance{ID: "old", Created: now.Add(-time.Minute), Status: "Success", Data: engine.Result{Bytes: maxInstanceBytes / 2}}
	s.instances["recent"] = &instance{ID: "recent", Created: now, Status: "Success", Data: engine.Result{Bytes: maxInstanceBytes / 2}}
	pending := &instance{ID: "new", Created: now, Status: "Running"}
	if !s.reserveInstance(pending) {
		t.Fatal("reserve failed")
	}
	completed := &instance{ID: "new", Created: now, Status: "Success", Output: "CSV consumes budget too"}
	s.publishInstance(completed)
	if s.instances["old"] != nil || s.instances["recent"] == nil || s.instances["new"] != completed {
		t.Fatal("must evict the oldest completed result and retain the new result")
	}
	var total int64
	for _, i := range s.instances {
		total += instanceBytes(i)
	}
	if total > maxInstanceBytes {
		t.Fatalf("instance budget exceeded: %d", total)
	}
}

func TestInstanceAdmissionReclaimsExpired(t *testing.T) {
	s := New(nil, Config{SessionTTL: time.Minute})
	for n := 0; n < 10000; n++ {
		s.instances[fmt.Sprint(n)] = &instance{Created: time.Now().Add(-time.Hour), Status: "Success"}
	}
	if !s.reserveInstance(&instance{ID: "new", Created: time.Now(), Status: "Running"}) || len(s.instances) != 1 {
		t.Fatal("expired instances must release admission capacity")
	}
}

func TestInstanceDownloadReclaimsExpiredSessions(t *testing.T) {
	for _, path := range []string{"/projects/p/instances/fresh?downloads", "/api/storage/v2?Target=projects.p.instances.fresh&Action=InstanceCreateReadSession"} {
		t.Run(path, func(t *testing.T) {
			s, h := fixture(t, Config{})
			for n := 0; n < 64; n++ {
				s.sessions[fmt.Sprint(n)] = &session{Created: time.Now().Add(-time.Hour)}
			}
			s.instances["fresh"] = &instance{ID: "fresh", Project: "p", Schema: "default", Status: "Success", Created: time.Now()}
			code, body := sendBody(t, h, "POST", path, nil, nil)
			if code != 200 {
				t.Fatalf("expired sessions blocked admission: %d %s", code, body)
			}
		})
	}
}

func TestInstanceDownloadSharesSnapshotBudget(t *testing.T) {
	s, h := fixture(t, Config{})
	s.sessions["large"] = &session{Created: time.Now(), Data: engine.Result{Bytes: 256 << 20}}
	s.instances["fresh"] = &instance{ID: "fresh", Project: "p", Schema: "default", Status: "Success", Created: time.Now(), Data: engine.Result{Bytes: 1}}
	code, body := sendBody(t, h, "POST", "/projects/p/instances/fresh?downloads", nil, nil)
	if code != 429 {
		t.Fatalf("expected shared byte budget rejection: %d %s", code, body)
	}
}
