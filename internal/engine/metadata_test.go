package engine

import (
	"context"
	"path/filepath"
	"testing"
)

func TestMetadataPartitionsPersistAndQualifiedNames(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "meta.duckdb")
	e, err := Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	run := func(q string) {
		t.Helper()
		if _, err := e.Execute(ctx, "p", "default", q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	run("create table t(id bigint) partitioned by(ds string)")
	run("alter table p.default.t add partition(ds='empty')")
	run("insert into t partition(ds='data') values(1)")
	run("insert overwrite table t partition(ds='data') select id from t where false")
	parts, err := e.Partitions(ctx, "p", "default", "t")
	if err != nil || len(parts) != 2 {
		t.Fatalf("overwrite lost partitions: %v %v", parts, err)
	}
	run("truncate table p.default.t")
	e.Close()
	e, err = Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	parts, err = e.Partitions(ctx, "p", "default", "t")
	if err != nil || len(parts) != 2 {
		t.Fatalf("restart lost partitions: %v %v", parts, err)
	}
	run("create table a(id bigint)")
	run("create table b(id bigint)")
	run("insert into a values(1)")
	run("insert into b values(1)")
	result, err := e.Execute(ctx, "p", "default", "select p.id from a p join b q on p.id=q.id")
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("project-named alias: %v %v", result, err)
	}
	if _, err = e.Execute(ctx, "p", "default", "alter table other.default.t add partition(ds='bad')"); err == nil {
		t.Fatal("cross-project DDL accepted")
	}
}

func TestProjectCatalogSurvivesRestartAndLastTableDrop(t *testing.T) {
	path := t.TempDir() + "/catalog.duckdb"
	e, err := Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err = e.Execute(ctx, "custom", "default", "create table t(id bigint);drop table t"); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Execute(ctx, "failed", "default", "create table t(id invalid_type)"); err == nil {
		t.Fatal("expected invalid type")
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for _, tc := range []struct {
		p    string
		want bool
	}{{"custom", true}, {"failed", false}, {"missing", false}} {
		got, err := e.HasProject(ctx, tc.p)
		if err != nil || got != tc.want {
			t.Fatalf("%s exists=%v err=%v", tc.p, got, err)
		}
	}
}
