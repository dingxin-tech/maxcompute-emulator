package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

const acidCopyDDL = "create table %s(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"

// rowsText renders a result as "col,col" per row, so a test failure names the rows.
func rowsText(r Result) string {
	out := []string{}
	for _, row := range r.Rows {
		cells := []string{}
		for _, v := range row {
			cells = append(cells, fmt.Sprint(v))
		}
		out = append(out, strings.Join(cells, ","))
	}
	return strings.Join(out, " ")
}

// TestDeltaInsertSelectStarKeepsVersionColumnsPrivate covers the review finding on the
// INSERT staging path: the input is snapshotted with CREATE TEMP TABLE ... AS <source>,
// and a star projection over a delta table carries the three hidden version columns, so
// the physical stage width no longer matched the user width and the copy failed with a
// column count mismatch. Letting them through would have been worse than failing: the
// inserted rows would arrive with the source row's version.
//
// Both shapes the review named are pinned, and the events each target publishes prove the
// copies landed in the target's own version space.
func TestDeltaInsertSelectStarKeepsVersionColumnsPrivate(t *testing.T) {
	ctx := context.Background()
	e := openTestEngine(t)
	for _, name := range []string{"src", "dst"} {
		if _, err := e.Execute(ctx, "p", "default", fmt.Sprintf(acidCopyDDL, name)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.Execute(ctx, "p", "default", "insert into src values (1,'a'),(2,'b')"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(ctx, "p", "default", "insert into dst select * from src"); err != nil {
		t.Fatalf("copy between two delta tables: %v", err)
	}
	rows, err := e.Execute(ctx, "p", "default", "select id, s from dst order by id")
	if err != nil {
		t.Fatal(err)
	}
	if got := rowsText(rows); got != "1,a 2,b" {
		t.Fatalf("copied rows=%s want the two source rows", got)
	}
	// The copy published two inserts in dst's own history. Had the hidden columns travelled
	// with the rows, dst would carry src's versions and its next write would collide with
	// them, which the cell below is there to catch.
	copied, cursor, err := e.DeltaRead(ctx, "p", "default", "dst", nil, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(opCodes(copied)); got != "[2 2]" {
		t.Fatalf("dst events=%s want two inserts", got)
	}
	if _, err = e.Execute(ctx, "p", "default", "insert into dst values (3,'c')"); err != nil {
		t.Fatal(err)
	}
	next, after, err := e.DeltaRead(ctx, "p", "default", "dst", nil, cursor, -1, true)
	if err != nil {
		t.Fatalf("resume after the copy: %v", err)
	}
	if got := fmt.Sprint(opCodes(next)); got != "[2]" {
		t.Fatalf("dst events after the copy=%s want the new insert at its own version", got)
	}
	if after <= cursor {
		t.Fatalf("dst cursor %d did not advance past %d: the copy reused the target's version space", after, cursor)
	}
	// Self-overwrite: the snapshot has to be taken before the delete, and the reinserted
	// rows republish as inserts above the delete events.
	if _, err = e.Execute(ctx, "p", "default", "insert overwrite table src select * from src"); err != nil {
		t.Fatalf("self overwrite of a delta table: %v", err)
	}
	kept, err := e.Execute(ctx, "p", "default", "select id, s from src order by id")
	if err != nil {
		t.Fatal(err)
	}
	if got := rowsText(kept); got != "1,a 2,b" {
		t.Fatalf("rows after self overwrite=%s want the table unchanged", got)
	}
	events, _, err := e.DeltaRead(ctx, "p", "default", "src", nil, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(opCodes(events)); got != "[2 2 4 4 2 2]" {
		t.Fatalf("src events=%s want two baseline inserts, two deletes, two reinserts", got)
	}
	// A plain read of a delta table still shows only user columns.
	if plain, err := e.Execute(ctx, "p", "default", "select * from src"); err != nil || len(plain.Columns) != 2 {
		t.Fatalf("plain star read columns=%d err=%v want the two user columns", len(plain.Columns), err)
	}
	// A non-delta target reading from a delta source goes through the same stage, so the
	// hidden columns have to be invisible there too.
	if _, err := e.Execute(ctx, "p", "default", "create table plain(id bigint, s string)"); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Execute(ctx, "p", "default", "insert into plain select * from src"); err != nil {
		t.Fatalf("copy from a delta table into a plain table: %v", err)
	}
	if out, err := e.Execute(ctx, "p", "default", "select id, s from plain order by id"); err != nil ||
		rowsText(out) != "1,a 2,b" {
		t.Fatalf("plain target rows=%v err=%v", rowsText(out), err)
	}
}
