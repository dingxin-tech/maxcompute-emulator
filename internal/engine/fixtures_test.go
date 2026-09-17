package engine

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestPublicSQLTypeFixtures(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	statements := []string{
		"create table fixture(ts timestamp_ntz,d date,dt datetime,n bigint,dec decimal(8,2),a array<bigint>,m map<string,bigint>,s struct<x:struct<y:bigint>>)",
		"insert into fixture values(cast('1969-12-31 23:59:59.123456789' as timestamp_ntz),cast('1960-01-02' as date),cast('1969-12-31 23:59:59.123' as datetime),-9223372036854775808,12.345,array(1,2),map('key',3),named_struct('x',named_struct('y',7)))",
		"insert into fixture values(NULL,NULL,NULL,9223372036854775807,NULL,array(),map(),NULL)",
	}
	for _, sql := range statements {
		if _, err = e.Execute(ctx, "p", "default", sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	r, _, err := e.Snapshot(ctx, "p", "default", "fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatal(r)
	}
	if r.Rows[0][0].(time.Time).Nanosecond() != 123456789 || fmt.Sprint(r.Rows[0][4]) != "12.35" {
		t.Fatal(r.Rows)
	}
	for _, sql := range []string{"insert into fixture values(NULL,NULL,NULL,9223372036854775808,NULL,NULL,NULL,NULL)", "insert into fixture values(NULL,NULL,NULL,0,9999999.99,NULL,NULL,NULL)"} {
		if _, err = e.Execute(ctx, "p", "default", sql); err == nil {
			t.Fatal("overflow accepted", sql)
		}
	}
	if _, err = e.Execute(ctx, "p", "default", "create table projected(k array<string>,v array<bigint>);insert into projected select map_keys(m),map_values(m) from fixture"); err != nil {
		t.Fatal("map projection", err)
	}
	projected, _, err := e.Snapshot(ctx, "p", "default", "projected", nil)
	if err != nil || len(projected.Rows) != 2 || fmt.Sprint(projected.Rows[0][0]) != "[key]" || fmt.Sprint(projected.Rows[0][1]) != "[3]" {
		t.Fatal(projected, err)
	}
	r, _, _ = e.Snapshot(ctx, "p", "default", "fixture", nil)
	if len(r.Rows) != 2 {
		t.Fatal("overflow committed")
	}
}

func TestPublishedTypeFixture(t *testing.T) {
	b, err := os.ReadFile("../../examples/types.sql")
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err = e.Execute(context.Background(), "p", "default", string(b)); err != nil {
		t.Fatal(err)
	}
	r, _, err := e.Snapshot(context.Background(), "p", "default", "e2e_types", nil)
	if err != nil || len(r.Rows) != 3 {
		t.Fatal(r, err)
	}
	if fmt.Sprint(r.Rows[1][4]) != "-12.35" || r.Rows[2][5] != nil || fmt.Sprint(r.Rows[0][8]) != "map[outer:map[inner:9]]" {
		t.Fatal(r.Rows)
	}
}

func TestNumericFixtureLimitsAndRescaling(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	sql := "create table limits(i tinyint,j smallint,k int,d decimal(8,2));insert into limits values(-128,-32768,-2147483648,-12.345),(127,32767,2147483647,cast(12.3 as decimal(8,1)))"
	if _, err = e.Execute(ctx, "p", "default", sql); err != nil {
		t.Fatal(err)
	}
	for _, values := range []string{"(128,0,0,0)", "(0,32768,0,0)", "(0,0,2147483648,0)", "(0,0,0,9999999.99)"} {
		if _, err = e.Execute(ctx, "p", "default", "insert into limits values"+values); err == nil {
			t.Fatal("overflow accepted", values)
		}
	}
	r, _, err := e.Snapshot(ctx, "p", "default", "limits", nil)
	if err != nil || len(r.Rows) != 2 || fmt.Sprint(r.Rows[0][3]) != "-12.35" || fmt.Sprint(r.Rows[1][3]) != "12.3" || r.Columns[3].Parsed.Scale != 2 {
		t.Fatal(r, err)
	}
}
