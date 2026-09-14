package engine

import (
	"context"
	"reflect"
	"testing"
)

func TestWriteNestedNullAndBinaryAtomicity(t *testing.T) {
	e, err := Open("", 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	if _, err = e.Execute(ctx, "p", "default", "create table t(a array<bigint>,m map<string,binary>,s struct<x:bigint,y:string>)"); err != nil {
		t.Fatal(err)
	}
	row := []any{[]any{int64(1), nil, int64(3)}, map[any]any{"z": []byte{0, 255, 39}, "a": nil}, map[string]any{"x": nil, "y": "quote's"}}
	if err = e.Write(ctx, "p", "default", "t", map[string]string{}, [][]any{row}, false, nil); err != nil {
		t.Fatal(err)
	}
	data, _, err := e.Snapshot(ctx, "p", "default", "t", map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(data.Rows[0], row) {
		t.Fatalf("round trip: %#v", data.Rows[0])
	}
	// All staged records are one transaction, including failure on the last row.
	bad := []any{[]any{"not a number"}, map[any]any{}, map[string]any{"x": int64(1), "y": "bad"}}
	if err = e.Write(ctx, "p", "default", "t", map[string]string{}, [][]any{row, bad}, false, nil); err == nil {
		t.Fatal("expected invalid conversion")
	}
	data, _, err = e.Snapshot(ctx, "p", "default", "t", map[string]string{})
	if err != nil || len(data.Rows) != 1 {
		t.Fatal("partial commit", err, len(data.Rows))
	}
}
