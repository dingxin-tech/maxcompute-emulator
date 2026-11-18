package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func execSQL(t *testing.T, e *Engine, q string) (Result, error) {
	t.Helper()
	return e.Execute(context.Background(), "p", "default", q)
}

// TestDeltaStarCopyAcrossSetOperations covers the review follow-up on the staging fix:
// CREATE TEMP TABLE ... AS <source> runs the WHOLE source query before dropStageHiddenColumns
// removes the version columns, so a set operation deduplicates over rows that still carry
// __em_lsn/__em_op/__em_ts - or fails on width before any cleanup can run. Dropping the
// columns afterwards cannot restore the SQL semantics; they have to be projected away before
// the relations meet.
//
// a and b hold the same user row (1,'x') at different committed versions: b burns versions 1
// and 2 on an insert plus a delete, so its row is version 3. Deduplication over the declared
// columns answers one row; over the physical rows it answers two.
func TestDeltaStarCopyAcrossSetOperations(t *testing.T) {
	e := openTestEngine(t)
	for _, n := range []string{"a", "b", "dst"} {
		if _, err := execSQL(t, e, fmt.Sprintf(acidCopyDDL, n)); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		"create table psrc(id bigint, s string)",
		"create table ptgt(id bigint, s string)",
		"create table u1(id bigint, s string)",
		"create table u2(id bigint, s string)",
		"create table u3(id bigint, s string)",
		"create table ptgt2(id bigint, s string)",
		"insert into a values (1,'x')",
		"insert into psrc values (1,'x')",
	} {
		if _, err := execSQL(t, e, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// b holds the same user row but reaches it at a later version: it is written, the table is
	// truncated (which consumes versions on the delete side) and the row is written again, so the
	// two tables cannot agree on __em_lsn. SQL DELETE is not an engine delete face (it answers
	// UnsupportedFeature) and an ordinary write appends rather than merging, which is why the
	// version is advanced this way.
	ctx := context.Background()
	if _, err := execSQL(t, e, "insert into b values (1,'x')"); err != nil {
		t.Fatal(err)
	}
	if _, err := execSQL(t, e, "truncate table b"); err != nil {
		t.Fatal(err)
	}
	if _, err := execSQL(t, e, "insert into b values (1,'x')"); err != nil {
		t.Fatal(err)
	}
	// The premise, measured rather than assumed: the two rows disagree on a hidden column, so
	// a UNION that compares them is not accidentally correct.
	av, abound, err := e.DeltaRead(ctx, "p", "default", "a", nil, 0, -1, true)
	if err != nil || len(av.Rows) != 1 {
		t.Fatalf("a events=%v bound=%d err=%v", opCodes(av), abound, err)
	}
	_, bbound, err := e.DeltaRead(ctx, "p", "default", "b", nil, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	// b 的活行只有一条，版本却比 a 高：这就是 UNION 比较整行时会多出的一行
	if one, _, err2 := e.DeltaRead(ctx, "p", "default", "b", nil, 0, -1, false); err2 != nil || len(one.Rows) != 1 {
		t.Fatalf("b live rows=%d err=%v want the single merged row", len(one.Rows), err2)
	}
	if bbound == abound {
		t.Fatalf("both tables are at version %d: the rows no longer differ on a hidden column", bbound)
	}

	// Cell 1: UNION over two delta tables answers the one row the declared columns deduplicate to.
	if _, err = execSQL(t, e, "insert into u1 select * from a union select * from b"); err != nil {
		t.Fatalf("UNION of two delta tables: %v", err)
	}
	got, err := execSQL(t, e, "select id, s from u1 order by id")
	if err != nil {
		t.Fatal(err)
	}
	if rows := rowsText(got); rows != "1,x" {
		t.Fatalf("UNION rows=%s want one row; two rows means the set operation compared the hidden version columns", rows)
	}

	// Cell 2: UNION ALL between a delta table and a plain table of the same user schema runs.
	if _, err = execSQL(t, e, "insert into u2 select * from a union all select * from psrc"); err != nil {
		t.Fatalf("UNION ALL delta + plain: %v", err)
	}
	got, err = execSQL(t, e, "select id, s from u2 order by id")
	if err != nil {
		t.Fatal(err)
	}
	if rows := rowsText(got); rows != "1,x 1,x" {
		t.Fatalf("UNION ALL rows=%s want both sides - UNION ALL keeps duplicates", rows)
	}

	// Cell 3: the same UNION into a delta target publishes exactly one insert event for the row
	// the consumer reads, and the next write still takes a version above the copy.
	if _, err = execSQL(t, e, "insert into dst select * from a union select * from b"); err != nil {
		t.Fatalf("UNION into a delta target: %v", err)
	}
	ev, cursor, err := e.DeltaRead(ctx, "p", "default", "dst", nil, 0, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(opCodes(ev)); got != "[2]" {
		t.Fatalf("dst events=%s want the single insert the deduplicated row publishes", got)
	}
	if rows, rerr := execSQL(t, e, "select id, s from dst"); rerr != nil || rowsText(rows) != "1,x" {
		t.Fatalf("dst rows=%s err=%v", rowsText(rows), rerr)
	}
	if _, err = execSQL(t, e, "insert into dst values (2,'y')"); err != nil {
		t.Fatal(err)
	}
	tail, advanced, err := e.DeltaRead(ctx, "p", "default", "dst", nil, cursor, -1, true)
	if err != nil || fmt.Sprint(opCodes(tail)) != "[2]" || advanced <= cursor {
		t.Fatalf("after the copy cursor=%d->%d events=%v err=%v", cursor, advanced, opCodes(tail), err)
	}

	// Cell 4: a star branch plus an explicitly projected branch is projectable, so it answers
	// the row the declared columns deduplicate to instead of stopping on the width difference.
	if _, err = execSQL(t, e, "insert into ptgt2 select * from a union select id as x, s as y from b order by x"); err != nil {
		t.Fatalf("star UNION explicit branch: %v", err)
	}
	if rows, rerr := execSQL(t, e, "select id, s from ptgt2"); rerr != nil || rowsText(rows) != "1,x" {
		t.Fatalf("ptgt2 rows=%s err=%v want one row", rowsText(rows), rerr)
	}
	// Cell 5: a star that cannot be attributed to exactly one relation stays un-simulated and is
	// refused, because the answer would otherwise depend on hidden columns being compared.
	if _, err = execSQL(t, e, "insert into u3 select * from a join b on a.id=b.id union select * from psrc"); err == nil ||
		!strings.Contains(err.Error(), "UnsupportedFeature") {
		t.Fatalf("star over a join inside a set operation: err=%v want an explicit UnsupportedFeature refusal", err)
	}
	if rows, rerr := execSQL(t, e, "select id, s from u3"); rerr != nil || len(rows.Rows) != 0 {
		t.Fatalf("a refused source must not stage anything, rows=%s err=%v", rowsText(rows), rerr)
	}
	// Nothing delta in sight: the plain-to-plain UNION keeps the behaviour it always had.
	if _, err = execSQL(t, e, "create table u4(id bigint, s string)"); err != nil {
		t.Fatal(err)
	}
	if _, err = execSQL(t, e, "insert into u4 select * from psrc union select * from ptgt"); err != nil {
		t.Fatalf("plain-only UNION must not be touched by the new rule: %v", err)
	}
	// psrc 有 (1,'x')，ptgt 还没写过行 ⇒ 这一格只核"没有 delta 参与就不碰"
	if rows, uerr := execSQL(t, e, "select id, s from u4"); uerr != nil || rowsText(rows) != "1,x" {
		t.Fatalf("plain-only UNION rows=%s err=%v want it untouched by the new rule", rowsText(rows), uerr)
	}
}
