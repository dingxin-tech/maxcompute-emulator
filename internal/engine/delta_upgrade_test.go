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
	if _, err = e2.Execute(ctx, "p", "default", "truncate table t"); err != nil {
		t.Fatalf("truncate on an upgraded table: %v", err)
	}
	res, _, err = e2.DeltaRead(ctx, "p", "default", "t", nil, latest, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(opCodes(res)); got != "[4 4 4]" {
		t.Fatalf("truncate events=%s want one delete per published row", got)
	}
}
