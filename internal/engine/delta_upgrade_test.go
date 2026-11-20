package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
)

// TestOpenUpgradesPersistedDeltaTables covers the second review finding: IsDelta() reads
// the catalog, so a transactional table stored by an older release is already treated as
// a delta table, while the hidden columns, the sequence and the history table were only
// ever built by the CREATE path. Such a table used to enter the new write, TRUNCATE and
// incremental-read paths and fail there on a missing column or a missing table.
//
// The pre-change layout is reproduced by taking a persisted file and dropping exactly the
// objects the old release never had, then reopening it through Engine.Open.
func TestOpenUpgradesPersistedDeltaTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.duckdb")
	e, err := Open(path, 20000)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err = e.Execute(ctx, "p", "default", "create table t(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Execute(ctx, "p", "default", "insert into t values (1,'a'),(2,'b')"); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	ns := namespace("p", "default")
	var raw string
	if err = db.QueryRow("SELECT definition FROM main.emulator_catalog WHERE namespace=? AND name=?", ns, "t").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var tbl Table
	if err = json.Unmarshal([]byte(raw), &tbl); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"ALTER TABLE " + Quote(ns) + "." + Quote("t") + " DROP COLUMN " + Quote(DeltaLSNColumn),
		"ALTER TABLE " + Quote(ns) + "." + Quote("t") + " DROP COLUMN " + Quote(DeltaOpColumn),
		"ALTER TABLE " + Quote(ns) + "." + Quote("t") + " DROP COLUMN " + Quote(DeltaTSColumn),
		"DROP TABLE IF EXISTS " + Quote(ns) + "." + Quote(tbl.DeltaLog()),
		"DROP SEQUENCE IF EXISTS " + Quote(ns) + "." + tbl.DeltaSequence(),
	} {
		if _, err = db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopening the file is what an older snapshot goes through on the next start.
	e2, err := Open(path, 20000)
	if err != nil {
		t.Fatalf("open after downgrade: %v", err)
	}
	defer e2.Close()

	// Ordinary writes, TRUNCATE and incremental reads must all work on the upgraded table.
	if err = e2.Write(ctx, "p", "default", "t", map[string]string{}, [][]any{{int64(3), "c"}}, false, nil); err != nil {
		t.Fatalf("ordinary write on an upgraded table: %v", err)
	}
	res, latest, err := e2.DeltaRead(ctx, "p", "default", "t", nil, 0, -1, true)
	if err != nil {
		t.Fatalf("incremental read on an upgraded table: %v", err)
	}
	// Explicit initial-history contract: the rows that existed before the upgrade are
	// published once as insert events at version 1, so no consumer is told the table was
	// empty; the write after the upgrade is a later version.
	if got := fmt.Sprint(opCodes(res)); got != "[2 2 2]" {
		t.Fatalf("events after upgrade=%s want three inserts (two baseline rows, one new write)", got)
	}
	if latest < 2 {
		t.Fatalf("latest=%d want the post-upgrade write to take a version above the baseline", latest)
	}

	// A repair that runs on every start has to be idempotent, and a version that a consumer
	// already stored cannot fall back. Closing and reopening the same file is what an
	// operator does on each restart, so the check happens while the rows are still live.
	if err = e2.Close(); err != nil {
		t.Fatalf("close before the second restart: %v", err)
	}
	e3, err := Open(path, 20000)
	if err != nil {
		t.Fatalf("second open of the same file: %v", err)
	}
	defer e3.Close()
	history, cursor, err := e3.DeltaRead(ctx, "p", "default", "t", nil, 0, -1, true)
	if err != nil {
		t.Fatalf("incremental read after the second restart: %v", err)
	}
	if got := fmt.Sprint(opCodes(history)); got != "[2 2 2]" {
		t.Fatalf("history after the second restart=%s want the three events of the first start, no republished baseline", got)
	}
	if cursor != latest {
		t.Fatalf("version after the second restart=%d want the %d the previous start reported (no regression)", cursor, latest)
	}
	// A write made after the restart must be reachable from the cursor the previous start
	// handed out, which is the whole point of not regressing it.
	if err = e3.Write(ctx, "p", "default", "t", map[string]string{}, [][]any{{int64(4), "d"}}, false, nil); err != nil {
		t.Fatalf("write after the second restart: %v", err)
	}
	tail2, advanced, err := e3.DeltaRead(ctx, "p", "default", "t", nil, cursor, -1, true)
	if err != nil {
		t.Fatalf("resume from the cursor of the previous start: %v", err)
	}
	if got := fmt.Sprint(opCodes(tail2)); got != "[2]" {
		t.Fatalf("events after the second restart=%s want the one new insert", got)
	}
	if advanced <= cursor {
		t.Fatalf("cursor went from %d to %d: a write after a restart did not take a new version", cursor, advanced)
	}
	if _, err = e3.Execute(ctx, "p", "default", "truncate table t"); err != nil {
		t.Fatalf("truncate on an upgraded table: %v", err)
	}
	res, afterTruncate, err := e3.DeltaRead(ctx, "p", "default", "t", nil, advanced, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(opCodes(res)); got != "[4 4 4 4]" {
		t.Fatalf("truncate events=%s want one delete per published row", got)
	}
	// A third start keeps the same history and the same version.
	if err = e3.Close(); err != nil {
		t.Fatalf("close before the third restart: %v", err)
	}
	e4, err := Open(path, 20000)
	if err != nil {
		t.Fatalf("third open of the same file: %v", err)
	}
	defer e4.Close()
	full, stable, err := e4.DeltaRead(ctx, "p", "default", "t", nil, 0, -1, true)
	if err != nil {
		t.Fatalf("incremental read after the third restart: %v", err)
	}
	if got := fmt.Sprint(opCodes(full)); got != "[2 2 2 2 4 4 4 4]" {
		t.Fatalf("history after the third restart=%s want eight events, four inserts and four deletes", got)
	}
	if stable != afterTruncate {
		t.Fatalf("version after the third restart=%d want %d", stable, afterTruncate)
	}
}
