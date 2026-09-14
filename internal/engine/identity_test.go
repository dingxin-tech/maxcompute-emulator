package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestTableIdentityMigratesOnceAndSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.duckdb")
	ctx := context.Background()
	e, err := Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.Execute(ctx, "p", "default", "create table t(id bigint)"); err != nil {
		t.Fatal(err)
	}
	e.Close()
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	var raw string
	if err = db.QueryRow("SELECT definition FROM main.emulator_catalog").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var old map[string]any
	json.Unmarshal([]byte(raw), &old)
	delete(old, "ID")
	b, _ := json.Marshal(old)
	if _, err = db.Exec("UPDATE main.emulator_catalog SET definition=?", string(b)); err != nil {
		t.Fatal(err)
	}
	db.Close()
	e, err = Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	first, err := e.Table(ctx, "p", "default", "t")
	if err != nil || first.ID == "" {
		t.Fatal(first.ID, err)
	}
	e.Close()
	e, err = Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	second, err := e.Table(ctx, "p", "default", "t")
	if err != nil || second.ID != first.ID {
		t.Fatal(second.ID, first.ID, err)
	}
	if _, err = e.Execute(ctx, "p", "default", "drop table t; create table t(id bigint)"); err != nil {
		t.Fatal(err)
	}
	replacement, err := e.Table(ctx, "p", "default", "t")
	if err != nil || replacement.ID == first.ID {
		t.Fatal(replacement.ID, err)
	}
}
