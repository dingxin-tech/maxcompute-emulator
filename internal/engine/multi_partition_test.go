package engine

import (
	"context"
	"testing"
)

func TestMultipleAddPartitionClausesAndRollback(t *testing.T) {
	e, err := Open(":memory:", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	for _, query := range []string{"create table t(id bigint) partitioned by(ds string)", "alter table t add if not exists partition(ds='a') partition(ds='b')", "alter table t add if not exists partition(ds='a') partition(ds='b')"} {
		if _, err = e.Execute(ctx, "p", "default", query); err != nil {
			t.Fatal(err)
		}
	}
	parts, err := e.Partitions(ctx, "p", "default", "t")
	if err != nil || len(parts) != 2 {
		t.Fatal(parts, err)
	}
	if _, err = e.Execute(ctx, "p", "default", "alter table t add partition(ds='c') partition(wrong='d')"); err == nil {
		t.Fatal("invalid clause accepted")
	}
	parts, err = e.Partitions(ctx, "p", "default", "t")
	if err != nil || len(parts) != 2 {
		t.Fatal("partial statement committed", parts, err)
	}
}
