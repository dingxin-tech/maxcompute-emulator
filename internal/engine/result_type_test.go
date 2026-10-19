package engine

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestNestedQueryResultTypes(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	r, err := e.Execute(context.Background(), "p", "default", "select array(1,2) as a, map('x', array(3,4)) as m, named_struct('name','person','ages',array(20,21)) as s")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"array<int>", "map<string,array<int>>", "struct<name:string,ages:array<int>>"}
	for i, typ := range want {
		parsed, err := ParseType(typ)
		if err != nil || !reflect.DeepEqual(r.Columns[i].Parsed, parsed) {
			t.Fatalf("column %d: %+v", i, r.Columns[i])
		}
	}
	if fmt.Sprint(r.Rows[0][0]) != "[1 2]" {
		t.Fatal(r.Rows)
	}
}
