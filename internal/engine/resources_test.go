package engine

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestResourceLifecycleAndCaseInsensitiveLookup(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	if _, err = e.Execute(ctx, "p", "default", "create table src(id bigint);"); err != nil {
		t.Fatal(err)
	}
	res, err := e.PutResource(ctx, "p", "default", "WordCount.py", "py", "udf", false, "", []byte("def f(): pass"), ResourceCreate)
	if err != nil {
		t.Fatal(err)
	}
	if res.Name != "WordCount.py" || res.Type != "PY" || res.Size != 13 {
		t.Fatalf("create returned %+v", res)
	}
	if _, err = e.PutResource(ctx, "p", "default", "wordcount.py", "py", "", false, "", []byte("x"), ResourceCreate); err == nil || !strings.HasPrefix(err.Error(), "ResourceAlreadyExists:") {
		t.Fatalf("duplicate create: %v", err)
	}
	got, content, err := e.ResourceContent(ctx, "p", "default", "wordcount.PY")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "WordCount.py" || !bytes.Equal(content, []byte("def f(): pass")) {
		t.Fatalf("read %+v %q", got, content)
	}
	if _, err = e.PutResource(ctx, "p", "default", "snapshot", "TABLE", "", false, "src", nil, ResourceCreate); err != nil {
		t.Fatal(err)
	}
	table, err := e.Resource(ctx, "p", "default", "snapshot")
	if err != nil || table.Type != "TABLE" || table.TableName != "src" || table.Size != 0 {
		t.Fatalf("table resource %+v: %v", table, err)
	}
	if _, _, err = e.ResourceContent(ctx, "p", "default", "snapshot"); err == nil || !strings.HasPrefix(err.Error(), "UnsupportedOperation:") {
		t.Fatalf("table resource content: %v", err)
	}
	if _, err = e.PutResource(ctx, "p", "default", "wordcount.py", "jar", "", false, "", []byte("payload"), ResourceReplace); err == nil || !strings.HasPrefix(err.Error(), "InvalidResourceType:") {
		t.Fatalf("type change on update must be rejected: %v", err)
	}
	if _, err = e.PutResource(ctx, "p", "default", "WordCount.py", "PY", "replaced", false, "", []byte("payload"), ResourceReplace); err != nil {
		t.Fatal(err)
	}
	all, err := e.Resources(ctx, "p", "default")
	if err != nil || len(all) != 2 {
		t.Fatalf("list %+v: %v", all, err)
	}
	if all[0].Name != "snapshot" || all[1].Name != "WordCount.py" || all[1].Comment != "replaced" || all[1].Updated < all[1].Created {
		t.Fatalf("unexpected listing %+v", all)
	}
	if err = e.DeleteResource(ctx, "p", "default", "WORDCOUNT.PY"); err != nil {
		t.Fatal(err)
	}
	if err = e.DeleteResource(ctx, "p", "default", "missing"); err == nil || !strings.HasPrefix(err.Error(), "NoSuchResource:") {
		t.Fatalf("delete missing: %v", err)
	}
}

func TestResourceValidationAndBudget(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	if _, err = e.PutResource(ctx, "p", "default", "", "file", "", false, "", []byte("x"), ResourceCreate); err == nil || !strings.HasPrefix(err.Error(), "InvalidParameter:") {
		t.Fatalf("empty name: %v", err)
	}
	if _, err = e.PutResource(ctx, "p", "default", "../escape", "file", "", false, "", []byte("x"), ResourceCreate); err == nil || !strings.HasPrefix(err.Error(), "InvalidParameter:") {
		t.Fatalf("path separator: %v", err)
	}
	if _, err = e.PutResource(ctx, "p", "default", "a.txt", "unknown", "", false, "", []byte("x"), ResourceCreate); err == nil || !strings.HasPrefix(err.Error(), "InvalidResourceType:") {
		t.Fatalf("bad type: %v", err)
	}
	// The namespace budget is shared with other resources and survives updates.
	content, total := MaxResourceContent, MaxResourceTotal
	MaxResourceContent, MaxResourceTotal = 64, 96
	defer func() { MaxResourceContent, MaxResourceTotal = content, total }()
	if _, err = e.PutResource(ctx, "p", "default", "big.bin", "file", "", false, "", bytes.Repeat([]byte("a"), 97), ResourceCreate); err == nil || !strings.HasPrefix(err.Error(), "ResourceOverSize:") {
		t.Fatalf("single payload limit: %v", err)
	}
	if _, err = e.PutResource(ctx, "p", "default", "one.bin", "file", "", false, "", bytes.Repeat([]byte("a"), 64), ResourceCreate); err != nil {
		t.Fatal(err)
	}
	if _, err = e.PutResource(ctx, "p", "default", "two.bin", "file", "", false, "", bytes.Repeat([]byte("b"), 33), ResourceCreate); err == nil || !strings.HasPrefix(err.Error(), "ResourceOverSize:") {
		t.Fatalf("namespace budget: %v", err)
	}
	if _, err = e.PutResource(ctx, "p", "default", "one.bin", "file", "", false, "", bytes.Repeat([]byte("c"), 32), ResourceReplace); err != nil {
		t.Fatalf("shrinking an existing resource must fit: %v", err)
	}
	if _, err = e.PutResource(ctx, "p", "default", "two.bin", "file", "", false, "", bytes.Repeat([]byte("d"), 65), ResourceReplace); err == nil || !strings.HasPrefix(err.Error(), "ResourceOverSize:") {
		t.Fatalf("growth beyond the budget: %v", err)
	}
	// Schemas keep their own namespace budget.
	if _, err = e.PutResource(ctx, "p", "other", "big.bin", "file", "", false, "", bytes.Repeat([]byte("x"), 64), ResourceCreate); err != nil {
		t.Fatalf("schema isolation: %v", err)
	}
}

func TestFunctionValidationAndResourceClosure(t *testing.T) {
	e, err := Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	if _, err = e.PutFunction(ctx, "p", "default", Function{Name: "wc", ClassType: "com.example.WC", Resources: []string{"missing.py"}}, false); err == nil || !strings.HasPrefix(err.Error(), "InvalidParameter:") {
		t.Fatalf("missing dependency: %v", err)
	}
	if _, err = e.PutResource(ctx, "p", "default", "wc.py", "py", "", false, "", []byte("x"), ResourceCreate); err != nil {
		t.Fatal(err)
	}
	if _, err = e.PutFunction(ctx, "p", "default", Function{Name: "wc", ClassType: "com.example.WC", Resources: []string{"WC.py", "wc.py"}}, false); err != nil {
		t.Fatal(err)
	}
	if _, err = e.PutFunction(ctx, "p", "default", Function{Name: "wc", ClassType: "x", Resources: []string{"WC.py"}}, false); err == nil || !strings.HasPrefix(err.Error(), "FunctionAlreadyExists:") {
		t.Fatalf("duplicate: %v", err)
	}
	f, err := e.Function(ctx, "p", "default", "WC")
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Resources) != 1 || f.Resources[0] != "WC.py" || f.Owner != "emulator" {
		t.Fatalf("unexpected function %+v", f)
	}
	// Deleting a referenced resource leaves the function in place; registration
	// is validated at create/update time only, like the service.
	if _, err = e.PutFunction(ctx, "p", "default", Function{Name: "sql_sum", SQLFunction: true, SQLText: "select sum(x) from t"}, false); err != nil {
		t.Fatalf("sql function: %v", err)
	}
	if _, err = e.PutFunction(ctx, "p", "default", Function{Name: "bad_sql", SQLFunction: true}, false); err == nil {
		t.Fatal("sql function without text accepted")
	}
	if _, err = e.PutFunction(ctx, "p", "default", Function{Name: "emb", Embedded: true, Language: "python", Code: "print(1)"}, false); err != nil {
		t.Fatalf("embedded function: %v", err)
	}
	if _, err = e.PutFunction(ctx, "p", "default", Function{Name: "emb2", Embedded: true, Language: "python"}, false); err == nil {
		t.Fatal("embedded function without code accepted")
	}
	all, err := e.Functions(ctx, "p", "default")
	if err != nil || len(all) != 3 {
		t.Fatalf("list %+v: %v", all, err)
	}
	if err = e.DeleteFunction(ctx, "p", "default", "sql_sum"); err != nil {
		t.Fatal(err)
	}
	if err = e.DeleteFunction(ctx, "p", "default", "sql_sum"); err == nil || !strings.HasPrefix(err.Error(), "NoSuchFunction:") {
		t.Fatalf("delete twice: %v", err)
	}
}
