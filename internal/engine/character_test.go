package engine

import (
	"context"
	"testing"
)

func TestCharacterLimitsAndUnicodeValues(t *testing.T) {
	for _, typ := range []string{"char(0)", "char(256)", "varchar(65536)", "varchar(1,2)"} {
		if _, err := ParseType(typ); err == nil {
			t.Fatalf("accepted %s", typ)
		}
	}
	e, err := Open(":memory:", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	for _, q := range []string{"CREATE TABLE chars(c CHAR(4),v VARCHAR(3))", "INSERT INTO chars VALUES ('xy  ','中文测试'),(NULL,NULL)"} {
		if _, err = e.Execute(ctx, "test_project", "default", q); err != nil {
			t.Fatal(err)
		}
	}
	r, err := e.Execute(ctx, "test_project", "default", "SELECT * FROM chars")
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0] != "xy" || r.Rows[0][1] != "中文测" || r.Rows[1][0] != nil {
		t.Fatalf("%#v", r.Rows)
	}
	if err = e.Write(ctx, "test_project", "default", "chars", nil, [][]any{{"abcdE", "😀中文四"}}, false, nil); err != nil {
		t.Fatal(err)
	}
	r, err = e.Execute(ctx, "test_project", "default", "SELECT * FROM chars")
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[2][0] != "abcd" || r.Rows[2][1] != "😀中文" {
		t.Fatalf("%#v", r.Rows)
	}
}
