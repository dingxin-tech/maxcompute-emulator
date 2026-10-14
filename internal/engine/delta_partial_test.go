package engine

import (
	"context"
	"fmt"
	"testing"
)

// TestDeltaPartialUpsertKeepsUnmentionedColumns pins the shape a real consumer sends:
// only the listed columns come from the request, the rest survive the replacement.
func TestDeltaPartialUpsertKeepsUnmentionedColumns(t *testing.T) {
	ctx := context.Background()
	e := openTestEngine(t)
	ddl := "create table pp(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"
	if _, err := e.Execute(ctx, "p", "default", ddl); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(ctx, "p", "default", "insert into pp values (7,'keep')"); err != nil {
		t.Fatal(err)
	}
	if err := e.WriteMutations(ctx, "p", "default", "pp", map[string]string{},
		[][]any{{int64(7), nil}}, false, []byte{'U'}, [][]int{{0}}); err != nil {
		t.Fatal(err)
	}
	after, err := e.Execute(ctx, "p", "default", "select id, s from pp")
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Rows) != 1 || fmt.Sprint(after.Rows[0][1]) != "keep" {
		t.Fatalf("partial upsert dropped an unmentioned column: %+v", after.Rows)
	}
	events, _, err := e.DeltaRead(ctx, "p", "default", "pp", nil, 1, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	kinds := []int64{}
	for _, row := range events.Rows {
		kinds = append(kinds, eventOp(row, 2))
	}
	if len(kinds) != 2 || kinds[0] != 16 || kinds[1] != 8 {
		t.Fatalf("partial upsert events=%v want update-before then update-after", kinds)
	}
	if fmt.Sprint(events.Rows[0][1]) != "keep" || fmt.Sprint(events.Rows[1][1]) != "keep" {
		t.Fatalf("partial upsert images lost the unmentioned column: %+v", events.Rows)
	}
}
