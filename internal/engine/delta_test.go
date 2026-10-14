package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func openTestEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := Open("", 20000)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

// eventOp reads the operation code of an event row: data columns, then op, then ts.
func eventOp(row []any, dataCols int) int64 {
	n, err := strconv.ParseInt(fmt.Sprint(row[dataCols]), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("event op %q: %v", fmt.Sprint(row[dataCols]), err))
	}
	return n
}

// TestDeltaVersionsStampEveryWritePath pins the version layer the incremental read
// depends on: SQL inserts, upserts and deletes all publish events, and none of the
// hidden columns leak into what a user can see.
func TestDeltaVersionsStampEveryWritePath(t *testing.T) {
	ctx := context.Background()
	e := openTestEngine(t)
	ddl := "create table d(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"
	if _, err := e.Execute(ctx, "p", "default", ddl); err != nil {
		t.Fatal(err)
	}
	tbl, err := e.Table(ctx, "p", "default", "d")
	if err != nil {
		t.Fatal(err)
	}
	if !tbl.IsDelta() {
		t.Fatalf("table with primary keys and transactional=true is not delta: %+v", tbl.Properties)
	}
	n := len(tbl.Columns)
	if _, err := e.Execute(ctx, "p", "default", "insert into d values (1,'a'),(2,'b')"); err != nil {
		t.Fatal(err)
	}
	// A star projection must look exactly like the declared schema.
	r, err := e.Execute(ctx, "p", "default", "select * from d")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Columns) != 2 || r.Columns[0].Name != "id" || r.Columns[1].Name != "s" {
		t.Fatalf("hidden columns leaked into SELECT *: %+v", r.Columns)
	}
	for _, row := range r.Rows {
		if len(row) != 2 {
			t.Fatalf("hidden values leaked into SELECT *: %+v", row)
		}
	}
	// The SELECT source of an INSERT has to keep working with hidden columns present.
	if _, err := e.Execute(ctx, "p", "default", "insert into d select 3,'c'"); err != nil {
		t.Fatal(err)
	}
	inserts, bound1, err := e.DeltaRead(ctx, "p", "default", "d", nil, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if bound1 != 3 || len(inserts.Rows) != 3 {
		t.Fatalf("inserts window=(0,%d] rows=%+v", bound1, inserts.Rows)
	}
	for _, row := range inserts.Rows {
		if eventOp(row, n) != 2 {
			t.Fatalf("plain insert event op=%v want 2", row[n])
		}
	}
	// An upsert over an existing key publishes UPDATE_BEFORE then UPDATE_AFTER.
	if err := e.Write(ctx, "p", "default", "d", map[string]string{}, [][]any{{int64(1), "a2"}}, false, []byte{'U'}); err != nil {
		t.Fatal(err)
	}
	updated, bound2, err := e.DeltaRead(ctx, "p", "default", "d", nil, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if bound2 <= bound1 {
		t.Fatalf("bound did not advance: %d -> %d", bound1, bound2)
	}
	ops := []int64{}
	for _, row := range updated.Rows {
		ops = append(ops, eventOp(row, n))
	}
	want := []int64{2, 2, 2, 16, 32} // measured: 32 is the service CDC form of update-after
	if len(ops) != len(want) {
		t.Fatalf("cdc events=%v want %v", ops, want)
	}
	for i := range want {
		if ops[i] != want[i] {
			t.Fatalf("event order=%v want %v", ops, want)
		}
	}
	// The before-image carries the stored value, not the sparse request.
	for _, row := range updated.Rows {
		if eventOp(row, n) == 16 && fmt.Sprint(row[1]) != "a" {
			t.Fatalf("update-before image=%v", row)
		}
	}
	// Append mode never shows removed or replaced images.
	appended, _, err := e.DeltaRead(ctx, "p", "default", "d", nil, 0, -1, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range appended.Rows {
		if op := eventOp(row, n); op == 16 || op == 4 {
			t.Fatalf("append mode published image event %v", op)
		}
	}
	// A delete is an event of its own and keeps the version moving.
	if err := e.Write(ctx, "p", "default", "d", map[string]string{}, [][]any{{int64(2), nil}}, false, []byte{'D'}); err != nil {
		t.Fatal(err)
	}
	deleted, bound3, err := e.DeltaRead(ctx, "p", "default", "d", nil, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	last := eventOp(deleted.Rows[len(deleted.Rows)-1], n)
	if last != 4 || bound3 <= bound2 {
		t.Fatalf("delete event=%d bound %d->%d", last, bound2, bound3)
	}
	// A window starting at the previous version sees only the new events.
	tail, _, err := e.DeltaRead(ctx, "p", "default", "d", nil, bound2, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(tail.Rows) != 1 || eventOp(tail.Rows[0], n) != 4 {
		t.Fatalf("window (%d,%d] = %+v", bound2, bound3, tail.Rows)
	}
}

// TestDeltaBoundSurvivesClearing checks the bound a reader advances to cannot move
// backwards when the newest rows are removed by truncate or overwrite.
func TestDeltaBoundSurvivesClearing(t *testing.T) {
	ctx := context.Background()
	e := openTestEngine(t)
	ddl := "create table c(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"
	if _, err := e.Execute(ctx, "p", "default", ddl); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(ctx, "p", "default", "insert into c values (1,'a'),(2,'b')"); err != nil {
		t.Fatal(err)
	}
	_, before, err := e.DeltaRead(ctx, "p", "default", "c", nil, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(ctx, "p", "default", "truncate table c"); err != nil {
		t.Fatal(err)
	}
	events, after, err := e.DeltaRead(ctx, "p", "default", "c", nil, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if after <= before {
		t.Fatalf("truncate did not publish a newer version: %d -> %d", before, after)
	}
	deletes := 0
	for _, row := range events.Rows {
		if eventOp(row, 2) == 4 {
			deletes++
		}
	}
	if deletes != 2 {
		t.Fatalf("truncate delete events=%d want 2: %+v", deletes, events.Rows)
	}
	// INSERT OVERWRITE publishes the rows it replaced the same way.
	if _, err := e.Execute(ctx, "p", "default", "insert overwrite table c select 9,'z'"); err != nil {
		t.Fatal(err)
	}
	overwrite, _, err := e.DeltaRead(ctx, "p", "default", "c", nil, after, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(overwrite.Rows) != 1 || eventOp(overwrite.Rows[0], 2) != 2 {
		t.Fatalf("overwrite window=%+v want one insert event", overwrite.Rows)
	}
}

// TestDeltaReadRejectsNonDeltaAndBadRanges pins the rejections: a plain table has no
// versions to read, and an inverted window is a client error rather than an empty 200.
func TestDeltaReadRejectsNonDeltaAndBadRanges(t *testing.T) {
	ctx := context.Background()
	e := openTestEngine(t)
	if _, err := e.Execute(ctx, "p", "default", "create table plain(id bigint, s string)"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.DeltaRead(ctx, "p", "default", "plain", nil, 0, -1, true); err == nil ||
		!strings.Contains(err.Error(), "Only ACID table support incremental read") {
		t.Fatalf("non-delta incremental read: %v", err)
	}
	if _, err := e.Execute(ctx, "p", "default", "create table pk(id bigint, s string, primary key(id))"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.DeltaRead(ctx, "p", "default", "pk", nil, 0, -1, true); err == nil ||
		!strings.Contains(err.Error(), "Only ACID table") {
		t.Fatalf("primary keys without transactional must not be delta: %v", err)
	}
	acid := "create table win(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"
	if _, err := e.Execute(ctx, "p", "default", acid); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(ctx, "p", "default", "insert into win values (1,'a')"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.DeltaRead(ctx, "p", "default", "win", nil, 5, 1, true); err == nil ||
		!strings.Contains(err.Error(), "invalid incremental range") {
		t.Fatalf("inverted window: %v", err)
	}
	if rows, version, err := e.DeltaRead(ctx, "p", "default", "win", nil, 0, -1, true); err != nil ||
		version != 1 || len(rows.Rows) != 1 {
		t.Fatalf("single-row window: version=%d rows=%+v", version, rows.Rows)
	}
}

// TestDeltaPartitionedWindowReadsSelectedPartitions matches the consumer that tracks
// the newest partition: the window is filtered by partition as well as by version.
func TestDeltaPartitionedWindowReadsSelectedPartitions(t *testing.T) {
	ctx := context.Background()
	e := openTestEngine(t)
	ddl := "create table pt(id bigint, primary key(id)) partitioned by (ds string) tblproperties('transactional'='true')"
	if _, err := e.Execute(ctx, "p", "default", ddl); err != nil {
		t.Fatal(err)
	}
	tbl, err := e.Table(ctx, "p", "default", "pt")
	if err != nil {
		t.Fatal(err)
	}
	if !tbl.IsDelta() {
		t.Fatalf("partitioned delta table not recognised: %+v", tbl)
	}
	for _, part := range []string{"1", "2"} {
		if err := e.Write(ctx, "p", "default", "pt", map[string]string{"ds": part},
			[][]any{{int64(1)}}, false, []byte{'U'}); err != nil {
			t.Fatalf("write partition %s: %v", part, err)
		}
	}
	events, bound, err := e.DeltaRead(ctx, "p", "default", "pt", []map[string]string{{"ds": "2"}}, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Rows) != 1 {
		t.Fatalf("partition filter returned %+v", events.Rows)
	}
	if got := eventOp(events.Rows[0], len(tbl.Columns)); got != 2 {
		t.Fatalf("first event into an empty partition should be an insert, got %d", got)
	}
	if events.Columns[len(events.Columns)-1].Name != "ds" {
		t.Fatalf("event schema must end with partition columns: %+v", events.Columns)
	}
	if bound <= 0 {
		t.Fatalf("bound=%d", bound)
	}
}
