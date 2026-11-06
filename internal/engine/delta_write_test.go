package engine

import (
	"context"
	"fmt"
	"strconv"
	"testing"
)

// TestDeltaOrdinaryWritePublishesEvents is the regression for the P1 finding on the
// incremental-read review: an ordinary write - the shape a Tunnel block commit and a
// Storage batch commit both use, ops == nil - inserted the live row and advanced the
// version bound but published nothing to the event log, so a CDC read of the window that
// contained those writes came back with a version and no events. The later update and
// delete in this test are here on purpose: they must not be the only events a consumer
// ever sees for rows it never received an insert for.
func TestDeltaOrdinaryWritePublishesEvents(t *testing.T) {
	e, err := Open("", 20000)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	if _, err = e.Execute(ctx, "p", "default", "create table t(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"); err != nil {
		t.Fatal(err)
	}
	if err = e.Write(ctx, "p", "default", "t", map[string]string{}, [][]any{{int64(1), "a"}, {int64(2), "b"}}, false, nil); err != nil {
		t.Fatal(err)
	}
	res, latest, err := e.DeltaRead(ctx, "p", "default", "t", nil, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if latest < 2 {
		t.Fatalf("latest=%d want at least the two published versions", latest)
	}
	if got := opCodes(res); len(got) != 2 || got[0] != 2 || got[1] != 2 {
		t.Fatalf("ordinary write published %v, want two insert events", got)
	}

	// A subsequent update and delete keep their own events and do not stand in for the
	// inserts that used to be missing.
	if err = e.Write(ctx, "p", "default", "t", map[string]string{}, [][]any{{int64(1), "z"}}, false, []byte{'U'}); err != nil {
		t.Fatal(err)
	}
	if err = e.Write(ctx, "p", "default", "t", map[string]string{}, [][]any{{int64(2), "b"}}, false, []byte{'D'}); err != nil {
		t.Fatal(err)
	}
	res, _, err = e.DeltaRead(ctx, "p", "default", "t", nil, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	got := opCodes(res)
	want := []int64{2, 2, 16, 32, 4}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("event stream=%v want %v (insert, insert, update-before, update-after, delete)", got, want)
	}

	// An overwrite logs the rows it drops as deletes before publishing its own inserts.
	if err = e.Write(ctx, "p", "default", "t", map[string]string{}, [][]any{{int64(9), "new"}}, true, nil); err != nil {
		t.Fatal(err)
	}
	res, _, err = e.DeltaRead(ctx, "p", "default", "t", nil, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	tail := opCodes(res)[len(want):]
	if fmt.Sprint(tail) != fmt.Sprint([]int64{4, 2}) {
		t.Fatalf("overwrite stream tail=%v want [4 2] (delete of the replaced row, then the insert)", tail)
	}
}

// asInt64 widens whatever the value scanner produced into an int64 so the assertion
// compares numbers, and reports an unexpected shape as -1 rather than panicking.
func asInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int16:
		return int64(x)
	case int8:
		return int64(x)
	case float64:
		return int64(x)
	case []byte:
		n, _ := strconv.ParseInt(string(x), 10, 64)
		return n
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return -1
}

// opCodes reads the CDC operation column out of a DeltaRead result.
func opCodes(r Result) []int64 {
	idx := -1
	for i, c := range r.Columns {
		if c.Name == MetaOpColumn {
			idx = i
		}
	}
	if idx < 0 {
		return nil
	}
	out := []int64{}
	for _, row := range r.Rows {
		out = append(out, asInt64(row[idx]))
	}
	return out
}
