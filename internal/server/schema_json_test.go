package server

import (
	"strings"
	"testing"
	"time"
)

// TestSchemaOfAResultWithoutColumnsIsAnArray pins the JSON an instance download session
// must return for a statement that produces no columns. A nil slice marshals as `null`,
// and TunnelTableSchema#getAsJsonArray fails on that with "Not a JSON Array: null", which
// the Java SDK surfaces as a TunnelException ("Invalid json content") that names neither
// the field nor the cause. Every DDL and every INSERT reaches this path: the JDBC driver
// opens a download session for the instance it just ran, whatever the statement was.
func TestSchemaOfAResultWithoutColumnsIsAnArray(t *testing.T) {
	s, h := fixture(t, Config{})
	s.instances["ddl"] = &instance{ID: "ddl", Project: "p", Schema: "default", Name: "jdbc_sql_task", Status: "Success", Created: time.Now()}
	code, body := sendBody(t, h, "POST", "/projects/p/instances/ddl?downloads", nil, nil)
	if code != 200 {
		t.Fatalf("create instance download: code=%d body=%s", code, body)
	}
	text := string(body)
	for _, want := range []string{`"columns":[]`, `"partitionKeys":[]`} {
		if !strings.Contains(text, want) {
			t.Fatalf("schema of a resultless statement must stay an array (%s): %s", want, text)
		}
	}
	if strings.Contains(text, "null") {
		t.Fatalf("a null where the Java SDK reads an array: %s", text)
	}
}

// TestTableSchemaMembersAreArrays is the same rule on the metadata plane: a non-partitioned
// table has no partitions, which is an empty list and not an absent one.
func TestTableSchemaMembersAreArrays(t *testing.T) {
	_, h := fixture(t, Config{})
	code, body := sendBody(t, h, "GET", "/projects/p/tables/t", nil, nil)
	if code != 200 {
		t.Fatalf("get table: code=%d body=%s", code, body)
	}
	// The embedded Schema is an XML-escaped JSON document.
	text := strings.ReplaceAll(string(body), "&#34;", "\"")
	for _, want := range []string{`"columns":[`, `"partitionKeys":[`} {
		if !strings.Contains(text, want) {
			t.Fatalf("embedded Table.Schema must keep %s an array: %s", want, text)
		}
	}
}
