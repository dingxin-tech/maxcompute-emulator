package server

import (
	"bytes"
	"encoding/json"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"net/http"
	"testing"
)

func TestStorageV1BatchSessionContract(t *testing.T) {
	_, h := fixture(t, Config{})
	path := "/api/storage/v1/projects/p/schemas/default/tables/t/sessions?session_type=batch_read"
	resp, err := h.Client().Post(h.URL+path, "application/json", bytes.NewBufferString(`{"RequiredDataColumns":["s"],"SplitOptions":{"SplitMode":"RowOffset"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || result["SessionType"] != "batch_read" {
		t.Fatalf("%d %+v", resp.StatusCode, result)
	}
	sid := result["SessionId"].(string)
	code, _, _ := request(t, h, "GET", "/api/storage/v1/projects/p/schemas/default/tables/t/sessions/"+sid+"?session_type=batch_read", "")
	if code != 200 {
		t.Fatal(code)
	}
	code, _, b := request(t, h, "GET", "/api/storage/v1/projects/p/schemas/default/tables/t/data?session_id="+sid+"&row_index=0&row_count=2&max_batch_rows=1", "ZSTD")
	if code != 200 || len(b) == 0 {
		t.Fatalf("%d %s", code, b)
	}
	dataPath := "/api/storage/v1/projects/p/schemas/default/tables/t/data?session_id=" + sid
	for _, tc := range []struct {
		query string
		rows  int
	}{
		{"&row_index=0&row_count=2&skip_row_num=1", 1},
		{"&row_index=0&row_count=2&skip_row_num=3", 0},
		{"&row_index=0&row_count=2&data_format_type=arrow&data_format_version=v5", 2},
	} {
		code, _, body := request(t, h, "GET", dataPath+tc.query, "LZ4_FRAME")
		if code != 200 {
			t.Fatalf("%d %s", code, body)
		}
		reader, err := ipc.NewReader(bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for reader.Next() {
			count += int(reader.RecordBatch().NumRows())
		}
		if reader.Err() != nil {
			t.Fatal(reader.Err())
		}
		reader.Release()
		if count != tc.rows {
			t.Fatalf("%s: got %d rows, want %d", tc.query, count, tc.rows)
		}
	}
	for _, query := range []string{
		"&row_index=2048&row_count=1", "&row_index=-1&row_count=1",
		"&row_index=0&row_count=-1", "&row_index=0&row_count=1&skip_row_num=-1",
		"&row_index=0&row_count=1&max_batch_rows=20001",
		"&row_index=0&row_count=1&data_format_type=protobuf",
		"&row_index=0&row_count=1&data_format_version=v4",
	} {
		code, _, _ := request(t, h, "GET", dataPath+query, "ZSTD")
		if code != 400 {
			t.Fatalf("%s: got %d", query, code)
		}
	}
	code, _, _ = request(t, h, "GET", dataPath+"&row_index=0&row_count=1", "gzip")
	if code != 400 {
		t.Fatal("unsupported compression", code)
	}
	for _, url := range []string{
		"/api/storage/v1/projects/p/schemas/default/tables/t/data?session_id=missing",
		"/api/storage/v1/projects/p/schemas/default/tables/t/data?session_id=" + sid + "&row_count=-1",
		"/api/storage/v1/projects/p/schemas/default/tables/t/data?session_id=" + sid + "&max_batch_rows=bad",
	} {
		code, _, _ := request(t, h, "GET", url, "")
		if code == http.StatusOK {
			t.Fatal("invalid request accepted", url)
		}
	}
}

func TestStorageV1ReadColumnOrder(t *testing.T) {
	_, h := fixture(t, Config{})
	resp, err := h.Client().Post(h.URL+"/api/storage/v1/projects/p/schemas/default/tables/t/sessions?session_type=batch_read", "application/json", bytes.NewBufferString(`{"RequiredDataColumns":["id","s"],"SplitOptions":{"SplitMode":"RowOffset"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatal(result)
	}
	path := "/api/storage/v1/projects/p/schemas/default/tables/t/data?session_id=" + result["SessionId"].(string) + "&row_index=0&row_count=1&data_columns=s,id"
	code, _, _ := request(t, h, "GET", path, "ZSTD")
	if code != 400 {
		t.Fatal("unordered columns accepted without opt-in", code)
	}
	code, _, body := request(t, h, "GET", path+"&data_columns_unordered=true", "ZSTD")
	if code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	reader, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if reader.Schema().Field(0).Name != "s" || reader.Schema().Field(1).Name != "id" {
		t.Fatal(reader.Schema())
	}
}

// The public SDK enables CrossPartition by default on both surfaces, and the
// emulator answers every read with one logical split that already spans the
// selected partitions. The flag therefore has no physical meaning here and is
// accepted rather than refused; the refusals that do change semantics (bucket,
// parallelism, bucket sort) stay pinned below.
func TestStorageReadPlanningProtocolBoundary(t *testing.T) {
	_, h := fixture(t, Config{})
	body := `{"SplitOptions":{"SplitMode":"Size","SplitNumber":268435456,"CrossPartition":true}}`
	for _, tc := range []struct {
		path   string
		status int
	}{
		{"/api/storage/v3?Action=TableCreateReadSession&Target=projects.p.schemas.default.tables.t", 200},
		{"/api/storage/v1/projects/p/schemas/default/tables/t/sessions?session_type=batch_read", 200},
	} {
		resp, err := h.Client().Post(h.URL+tc.path, "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.status {
			t.Fatalf("%s: got %d, want %d", tc.path, resp.StatusCode, tc.status)
		}
	}
	for _, split := range []string{
		`{"SplitMode":"Bucket"}`,
		`{"SplitMode":"Parallelism","SplitNumber":4}`,
		`{"SplitMode":"Size","BucketSort":true}`,
	} {
		resp, err := h.Client().Post(h.URL+"/api/storage/v3?Action=TableCreateReadSession&Target=projects.p.schemas.default.tables.t", "application/json", bytes.NewBufferString(`{"SplitOptions":`+split+`}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatalf("unsupported split accepted: %s status %d", split, resp.StatusCode)
		}
	}
}
