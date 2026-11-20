package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestDeltaReadFutureEndIsRefusedLikeTheService replaces the capped-window choice made on
// 11-11 with what the service actually does. The defect the review found is unchanged: a
// caller that asked for (0,100] on a table at version 2 used to get LatestVersion=100 back,
// stored it, and lost every version between the bound and its number.
//
// The live measurement (one project, one throwaway partitioned ACID table, lifecycle 1,
// 2026-11-12) says the service does not clamp - it refuses: (0,2] answers two events while
// (0,3] and (0,1002] both fail with "The query version exceeded timetravel range". So the
// emulator refuses too, keeping the number a consumer stores inside what was committed.
func TestDeltaReadFutureEndIsRefusedLikeTheService(t *testing.T) {
	ctx := context.Background()
	e := openTestEngine(t)
	if _, err := e.Execute(ctx, "p", "default",
		"create table fw(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(ctx, "p", "default", "insert into fw values (1,'a'),(2,'b')"); err != nil {
		t.Fatal(err)
	}
	// The window the service answers: end exactly at the committed bound.
	exact, bound, err := e.DeltaRead(ctx, "p", "default", "fw", nil, 0, 2, true)
	if err != nil {
		t.Fatalf("window ending at the bound: %v", err)
	}
	if bound != 2 || fmt.Sprint(opCodes(exact)) != "[2 2]" {
		t.Fatalf("exact window cursor=%d events=%v want 2 and two inserts", bound, opCodes(exact))
	}
	// Over-future ends, both far and by one, are refused - the same two cells the probe measured.
	for _, end := range []int64{3, 100} {
		res, cursor, err := e.DeltaRead(ctx, "p", "default", "fw", nil, 0, end, true)
		if err == nil {
			t.Fatalf("end=%d accepted (cursor=%d rows=%d): the service refuses a window past the committed bound", end, cursor, len(res.Rows))
		}
		if !strings.Contains(err.Error(), "exceeds the committed version 2") {
			t.Fatalf("end=%d refused with %q, want the committed-bound message", end, err)
		}
	}
	// A refusal must not consume anything: the committed versions stay reachable, and the one
	// committed after the refused request is still ahead of the cursor the reader holds.
	if _, err = e.Execute(ctx, "p", "default", "insert into fw values (3,'c')"); err != nil {
		t.Fatal(err)
	}
	next, after, err := e.DeltaRead(ctx, "p", "default", "fw", nil, 2, -1, true)
	if err != nil {
		t.Fatalf("resume at the last committed cursor: %v", err)
	}
	if got := fmt.Sprint(opCodes(next)); got != "[2]" {
		t.Fatalf("events after the refused window=%s want the version-3 insert the consumer must not lose", got)
	}
	if after != 3 {
		t.Fatalf("cursor after resume=%d want 3", after)
	}
	// A start above the bound cannot be served either.
	if _, _, err = e.DeltaRead(ctx, "p", "default", "fw", nil, 5, 100, true); err == nil {
		t.Fatal("a start above the committed bound was accepted")
	}
	// A negative end still means "up to now" and is unaffected.
	if _, open, err := e.DeltaRead(ctx, "p", "default", "fw", nil, 0, -1, true); err != nil || open != 3 {
		t.Fatalf("open-ended read cursor=%d err=%v want 3", open, err)
	}
}
