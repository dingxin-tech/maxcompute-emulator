package engine

import (
	"context"
	"strings"
	"testing"
)

// TestLatestVersionFunctionMatchesSessionCutoff pins what a delta consumer needs: the
// version it polls and the version an incremental session reports are the same number, so
// a stored cursor can become the next window start without stalling or skipping rows.
func TestLatestVersionFunctionMatchesSessionCutoff(t *testing.T) {
	ctx := context.Background()
	e := openTestEngine(t)
	ddl := "create table lv(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"
	if _, err := e.Execute(ctx, "p", "default", ddl); err != nil {
		t.Fatal(err)
	}
	poll := func() int64 {
		t.Helper()
		r, err := e.Execute(ctx, "p", "default", "select get_latest_version('p.default.lv')")
		if err != nil {
			t.Fatal(err)
		}
		if r.Columns[0].Name != "_c0" {
			t.Fatalf("unexpected column name %v", r.Columns[0].Name)
		}
		v, _ := r.Rows[0][0].(int64)
		return v
	}
	if got := poll(); got != 0 {
		t.Fatalf("an empty ACID table answered %d", got)
	}
	if _, err := e.Execute(ctx, "p", "default", "insert into lv values (1,'a'),(2,'b')"); err != nil {
		t.Fatal(err)
	}
	if got := poll(); got != 2 {
		t.Fatalf("version after two inserted rows = %d", got)
	}
	_, version, err := e.DeltaRead(ctx, "p", "default", "lv", nil, 0, -1, true)
	if err != nil || version != poll() {
		t.Fatalf("session reported %d but the function says %d (err %v)", version, poll(), err)
	}
	// The probe step a consumer takes before planning a window: start at the polled
	// version, read nothing, and still get that version back as the cutoff.
	if rows, cutoff, err := e.DeltaRead(ctx, "p", "default", "lv", nil, version, -1, true); err != nil ||
		cutoff != version || len(rows.Rows) != 0 {
		t.Fatalf("probe window: cutoff=%d rows=%d err=%v", cutoff, len(rows.Rows), err)
	}
	if _, err := e.Execute(ctx, "p", "default", "insert into lv values (3,'c')"); err != nil {
		t.Fatal(err)
	}
	next, cutoff, err := e.DeltaRead(ctx, "p", "default", "lv", nil, version, -1, true)
	if err != nil || cutoff != poll() || len(next.Rows) != 1 {
		t.Fatalf("next window: cutoff=%d want %d rows=%d err=%v", cutoff, poll(), len(next.Rows), err)
	}
	// A plain table has no versions, and an unresolvable name is refused instead of
	// quietly answering zero.
	if _, err := e.Execute(ctx, "p", "default", "create table plainlv(id bigint, s string)"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(ctx, "p", "default", "select get_latest_version('p.default.plainlv')"); err == nil ||
		!strings.Contains(err.Error(), "not an ACID table") {
		t.Fatalf("plain table: %v", err)
	}
	if _, err := e.Execute(ctx, "p", "default", "select get_latest_version('a.b.c.d')"); err == nil ||
		!strings.Contains(err.Error(), "invalid table name") {
		t.Fatalf("four parts: %v", err)
	}
}
