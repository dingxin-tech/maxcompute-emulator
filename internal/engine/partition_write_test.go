package engine

import (
	"context"
	"path/filepath"
	"testing"
)

func TestWritePartitionCatalogSurvivesLastRowDeletionAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.duckdb")
	e, err := Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.Execute(ctx, "p", "default", "create table t(id bigint, primary key(id)) partitioned by(ds string)"); err != nil {
		t.Fatal(err)
	}
	// Dynamic writes register every materialized partition, including multiple values.
	if err = e.Write(ctx, "p", "default", "t", nil, [][]any{{int64(1), "a"}, {int64(2), "b"}}, false, nil); err != nil {
		t.Fatal(err)
	}
	if err = e.Write(ctx, "p", "default", "t", map[string]string{"ds": "a"}, [][]any{{int64(1)}}, false, []byte{'D'}); err != nil {
		t.Fatal(err)
	}
	if err = e.Write(ctx, "p", "default", "t", map[string]string{"ds": "b"}, nil, true, nil); err != nil {
		t.Fatal(err)
	}
	e.Close()
	e, err = Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	parts, err := e.Partitions(ctx, "p", "default", "t")
	if err != nil || len(parts) != 2 || parts[0]["ds"] != "a" || parts[1]["ds"] != "b" {
		t.Fatal(parts, err)
	}
	result, err := e.Execute(ctx, "p", "default", "select * from t")
	if err != nil || len(result.Rows) != 0 {
		t.Fatal(result.Rows, err)
	}
	if _, err = e.Execute(ctx, "p", "default", "alter table t drop partition(ds='a')"); err != nil {
		t.Fatal(err)
	}
	parts, err = e.Partitions(ctx, "p", "default", "t")
	if err != nil || len(parts) != 1 || parts[0]["ds"] != "b" {
		t.Fatal(parts, err)
	}
	// A transaction with one valid row followed by a bad cast must not retain a phantom partition.
	err = e.Write(ctx, "p", "default", "t", nil, [][]any{{int64(3), "c"}, {"bad-bigint", "d"}}, false, nil)
	if err == nil {
		t.Fatal("expected rollback")
	}
	parts, err = e.Partitions(ctx, "p", "default", "t")
	if err != nil || len(parts) != 1 {
		t.Fatal(parts, err)
	}
}
