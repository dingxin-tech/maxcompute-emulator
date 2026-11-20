package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

// TestDeltaTableInsideManagedSchema pins a combination that landed on two separate
// surfaces: a delta (ACID) table created inside a non-default managed schema, and the
// version window incremental reads answer from. The hidden sequence and the event log
// have to resolve in the table's own schema, the star projection has to stay clean
// there, and the cursor a consumer polls with SQL has to be the number the session
// publishes.
func TestDeltaTableInsideManagedSchema(t *testing.T) {
	s, h := fixture(t, Config{})
	if code, body := sendBody(t, h, "POST", "/projects/p/schemas", []byte("<Schema><Name>alpha</Name></Schema>"), nil); code != 201 {
		t.Fatalf("create schema: code=%d body=%s", code, body)
	}
	ctx := context.Background()
	if _, err := s.Engine.Execute(ctx, "p", "alpha", fmt.Sprintf(acidDDL, "acidalpha")); err != nil {
		t.Fatalf("create delta table in alpha: %v", err)
	}
	if _, err := s.Engine.Execute(ctx, "p", "alpha", "insert into acidalpha values (1,'v1'),(2,'v2'),(3,'v3')"); err != nil {
		t.Fatalf("insert into alpha delta table: %v", err)
	}

	res, err := s.Engine.Execute(ctx, "p", "alpha", "select * from acidalpha")
	if err != nil {
		t.Fatalf("star projection in alpha: %v", err)
	}
	for _, c := range res.Columns {
		if engine.IsHiddenColumn(c.Name) {
			t.Fatalf("hidden column %q visible in schema alpha", c.Name)
		}
	}
	if len(res.Rows) != 3 {
		t.Fatalf("live rows in alpha=%d want 3", len(res.Rows))
	}

	poll, err := s.Engine.Execute(ctx, "p", "alpha", "select get_latest_version('p.alpha.acidalpha')")
	if err != nil {
		t.Fatalf("get_latest_version in alpha: %v", err)
	}
	if len(poll.Rows) != 1 || fmt.Sprint(poll.Rows[0][0]) != "3" {
		t.Fatalf("get_latest_version rows=%v want 3", poll.Rows)
	}

	code, out, sid := openSession(t, h, "/api/storage/v2?Action=TableCreateReadSession&Target=projects.p.schemas.alpha.tables.acidalpha",
		"{\"IncrementalRead\":true,\"IncrementalReadOptions\":{\"StartVersion\":0,\"EndVersion\":-1,\"Mode\":\"cdc\"}}")
	if code != http.StatusOK || sid == "" {
		t.Fatalf("v2 incremental session in alpha: code=%d body=%v", code, out)
	}
	if version, _ := out["LatestVersion"].(float64); version != 3 {
		t.Fatalf("v2 LatestVersion in alpha=%v want 3", out["LatestVersion"])
	}
	schema, _ := out["DataSchema"].(map[string]any)
	if names, _ := eventColumns(t, schema); len(names) != 2 {
		t.Fatalf("v2 system columns in alpha=%v", names)
	}

	req, err := http.NewRequest(http.MethodPost, h.URL+"/api/storage/v2?Action=TableRead&Target=projects.p.schemas.alpha.tables.acidalpha&SessionId="+sid+"&Index=0", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "ZSTD")
	resp, err := h.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("v2 read in alpha: code=%d body=%s", resp.StatusCode, raw)
	}
	reader, err := ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("v2 read payload in alpha: %v %s", err, raw)
	}
	defer reader.Release()
	rows := 0
	for reader.Next() {
		rows += int(reader.RecordBatch().NumRows())
	}
	if reader.Err() != nil {
		t.Fatal(reader.Err())
	}
	if rows != 3 {
		t.Fatalf("v2 event rows in alpha=%d want 3", rows)
	}

	// Negative control: the same table name under the schema it was not created in
	// must not be answered. Without this, a passing alpha result would not prove the
	// session resolves by schema rather than ignoring it.
	code, out, sid = openSession(t, h, "/api/storage/v2?Action=TableCreateReadSession&Target=projects.p.schemas.default.tables.acidalpha",
		"{\"IncrementalRead\":true,\"IncrementalReadOptions\":{\"StartVersion\":0,\"EndVersion\":-1,\"Mode\":\"cdc\"}}")
	if code == http.StatusOK && sid != "" {
		t.Fatalf("cross-schema leak: default.acidalpha answered code=%d body=%v", code, out)
	}
	t.Logf("cross-schema refusal observed: code=%d", code)

	// And the SQL-side poll refuses the wrong schema instead of answering 0.
	if _, err := s.Engine.Execute(ctx, "p", "alpha", "select get_latest_version('p.default.acidalpha')"); err == nil {
		t.Fatalf("get_latest_version answered a table that is not in default")
	}
}
