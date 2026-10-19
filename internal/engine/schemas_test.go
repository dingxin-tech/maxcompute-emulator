package engine

import (
	"context"
	"errors"
	"testing"
)

func TestSchemaPersistenceAndIsolation(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/schemas.duckdb"
	e, err := Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.CreateSchema(ctx, "p", "ALPHA", "comment", false); err != nil {
		t.Fatal(err)
	}
	if err = e.CreateSchema(ctx, "q", "alpha", "other project", false); err != nil {
		t.Fatal(err)
	}
	if err = e.CreateSchema(ctx, "p", "alpha", "replace", false); !errors.Is(err, ErrSchemaExists) {
		t.Fatal(err)
	}
	if err = e.CreateSchema(ctx, "p", "alpha", "replace", true); err != nil {
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
	schemas, err := e.Schemas(ctx, "p")
	if err != nil || len(schemas) != 2 || schemas[0].Comment != "comment" {
		t.Fatal(schemas, err)
	}
	if _, err = e.Execute(ctx, "p", "default", "create table p.alpha.t(id bigint); create table root_table(id string)"); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Execute(ctx, "p", "ALPHA", "insert into t values(73L)"); err != nil {
		t.Fatal("uppercase current schema", err)
	}
	rows, _, err := e.Snapshot(ctx, "p", "alpha", "t", nil)
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(73) {
		t.Fatal("case variants have different storage", rows, err)
	}
	if _, err = e.Table(ctx, "p", "default", "root_table"); err != nil {
		t.Fatal("explicit schema leaked into next statement", err)
	}
	shown, err := e.Execute(ctx, "p", "default", "show schemas in p")
	if err != nil || len(shown.Rows) != 2 || shown.Rows[0][0] != "alpha" {
		t.Fatal(shown, err)
	}
	if err = e.DeleteSchema(ctx, "p", "alpha"); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Execute(ctx, "p", "alpha", "create table resurrected(id bigint)"); !errors.Is(err, ErrSchemaMissing) {
		t.Fatal("deleted schema was revived", err)
	}
	if _, err = e.PutResource(ctx, "p", "alpha", "r.py", "py", "", false, "", nil, ResourceCreate); !errors.Is(err, ErrSchemaMissing) {
		t.Fatal("resource revived deleted schema", err)
	}
	if _, err = e.Table(ctx, "p", "alpha", "t"); err == nil {
		t.Fatal("cascade left a table")
	}

	if err = e.DeleteSchema(ctx, "p", "alpha"); err != nil {
		t.Fatal(err)
	}
	if err = e.DeleteSchema(ctx, "p", "alpha"); err != nil {
		t.Fatal(err)
	}
	if err = e.DeleteSchema(ctx, "p", "default"); err == nil {
		t.Fatal("deleted default schema")
	}
	schemas, err = e.Schemas(ctx, "q")
	if err != nil || len(schemas) != 2 {
		t.Fatal(schemas, err)
	}
}

func TestExplicitInsertDoesNotRebindSourceSchema(t *testing.T) {
	ctx := context.Background()
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err = e.CreateSchema(ctx, "p", "alpha", "", false); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Execute(ctx, "p", "default", "create table src(id bigint);insert into src values(1);create table p.alpha.src(id bigint);insert into p.alpha.src values(2);create table p.alpha.dst(id bigint)"); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Execute(ctx, "p", "default", "insert into p.alpha.dst select * from src"); err == nil {
		t.Fatal("cross-schema INSERT SELECT accepted")
	}
	rows, _, err := e.Snapshot(ctx, "p", "alpha", "dst", nil)
	if err != nil || len(rows.Rows) != 0 {
		t.Fatal("rejected insert changed the destination", rows, err)
	}
	if _, err = e.Execute(ctx, "p", "default", "insert into p.alpha.dst values(3)"); err != nil {
		t.Fatal(err)
	}
	rows, _, err = e.Snapshot(ctx, "p", "alpha", "dst", nil)
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(3) {
		t.Fatal(rows, err)
	}
}
