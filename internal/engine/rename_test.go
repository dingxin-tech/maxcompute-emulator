package engine

import (
	"context"
	"testing"
)

func TestRenamePreservesIdentityPartitionsPropertiesAndRows(t *testing.T) {
	e, err := Open(":memory:", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	for _, q := range []string{"CREATE TABLE before(id BIGINT) PARTITIONED BY(ds STRING) TBLPROPERTIES('fixture'='value')", "ALTER TABLE before ADD PARTITION(ds='a')", "INSERT INTO before PARTITION(ds='a') VALUES (7)"} {
		if _, err = e.Execute(ctx, "p", "default", q); err != nil {
			t.Fatal(err)
		}
	}
	before, err := e.Table(ctx, "p", "default", "before")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.Execute(ctx, "p", "default", "ALTER TABLE before RENAME TO after"); err != nil {
		t.Fatal(err)
	}
	after, err := e.Table(ctx, "p", "default", "after")
	if err != nil || before.ID != after.ID || after.Properties["fixture"] != "value" || len(after.EmptyPartitions) != len(before.EmptyPartitions) {
		t.Fatal(after, err)
	}
	if _, err = e.Table(ctx, "p", "default", "before"); err == nil {
		t.Fatal("old name survived")
	}
	r, err := e.Execute(ctx, "p", "default", "SELECT * FROM after")
	if err != nil || len(r.Rows) != 1 || r.Rows[0][0] != int64(7) {
		t.Fatal(r, err)
	}
	if _, err = e.Execute(ctx, "p", "default", "ALTER TABLE after RENAME TO after"); err == nil {
		t.Fatal("collision accepted")
	}
}
