package engine

import (
	"context"
	"testing"
)

func TestPartitionsAtomicityAndIsolation(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	run := func(p, q string) {
		t.Helper()
		if _, err := e.Execute(ctx, p, "default", q); err != nil {
			t.Fatal(err)
		}
	}
	run("p", "create table t(id bigint,s string) partitioned by(ds string); insert into t partition(ds='a') values(1,'AbC'); insert into t partition(ds='b') values(2,'Other');")
	part, err := ParsePartition("ds='a'")
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := e.Snapshot(ctx, "p", "default", "t", part)
	if err != nil || len(r.Rows) != 1 || r.Rows[0][1] != "AbC" {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err = e.Execute(ctx, "p", "default", "insert overwrite table t partition(ds='a') values('bad','x')"); err == nil {
		t.Fatal("expected bad integer")
	}
	r, _, err = e.Snapshot(ctx, "p", "default", "t", part)
	if err != nil || r.Rows[0][0] != int64(1) {
		t.Fatal(r, err)
	}
	run("q", "create table t(id bigint)")
	if _, _, err = e.Snapshot(ctx, "q", "default", "t", part); err == nil {
		t.Fatal("partition must not cross projects")
	}
}
func TestOverwriteSelfAndUnsupported(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	if _, err = e.Execute(ctx, "p", "s", "create table t(id bigint);insert into t values(1);insert overwrite table t select id+1 from t;"); err != nil {
		t.Fatal(err)
	}
	r, _, err := e.Snapshot(ctx, "p", "s", "t", nil)
	if err != nil || r.Rows[0][0] != int64(2) {
		t.Fatal(r, err)
	}
	for _, q := range []string{"select * from read_csv('/etc/passwd')", "select from ;", "set threads=999;"} {
		if _, err = e.Execute(ctx, "p", "s", q); err == nil {
			t.Fatal(q)
		}
	}
}

func TestPersistenceAndReservedNamespaces(t *testing.T) {
	path := t.TempDir() + "/data.duckdb"
	e, err := Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err = e.Execute(ctx, "p", "s", "create table t(id bigint); insert into t values(42)"); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	r, _, err := e.Snapshot(ctx, "p", "s", "t", nil)
	if err != nil || r.Rows[0][0] != int64(42) {
		t.Fatal(r, err)
	}
	for _, q := range []string{"select * from main.emulator_catalog", "select * from information_schema.tables", "select * from duckdb_tables()", "select * from query('select 1')"} {
		if _, err = e.Execute(ctx, "p", "s", q); err == nil {
			t.Fatal("reserved access accepted:", q)
		}
	}
	if _, _, err = e.Snapshot(ctx, "p", "other", "t", nil); err == nil {
		t.Fatal("schema isolation failed")
	}
}
