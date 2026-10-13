package server

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func TestLostUploadAcknowledgementUsesOrdinaryRetryIdentity(t *testing.T) {
	s, h := fixture(t, Config{TestMode: true})
	ctx := context.Background()
	s.Engine.Execute(ctx, "p", "default", "CREATE TABLE writes(id BIGINT,s STRING)")
	code, b := sendBody(t, h, "POST", "/projects/p/tables/writes/streams", nil, nil)
	if code != 200 {
		t.Fatal(code, string(b))
	}
	var session map[string]any
	json.Unmarshal(b, &session)
	path := "/projects/p/tables/writes/streams?uploadid=" + session["session_name"].(string)
	cols := []engine.Column{}
	for _, field := range [][2]string{{"id", "bigint"}, {"s", "string"}} {
		c, _ := engine.NewColumn(field[0], field[1])
		cols = append(cols, c)
	}
	payload, err := wire.Protobuf(engine.Result{Columns: cols, Rows: [][]any{{int64(7), "retry"}}})
	if err != nil {
		t.Fatal(err)
	}
	putFault(t, h, "lost", FaultRule{Match: FaultMatch{Plane: "tunnel", Object: "stream", Action: "write", Project: "p", Table: "writes"}, Effect: FaultEffect{Type: "lost_ack"}, Times: 1})
	req, _ := http.NewRequest("PUT", h.URL+path, bytes.NewReader(payload))
	req.Header.Set("odps-tunnel-retry-trace-id", "same-request")
	resp, err := h.Client().Do(req)
	if err == nil {
		resp.Body.Close()
		t.Fatal("ack was not lost")
	}
	code, b = sendBody(t, h, "PUT", path, payload, map[string]string{"odps-tunnel-retry-trace-id": "same-request"})
	if code != 200 {
		t.Fatal(code, string(b))
	}
	data, _, err := s.Engine.Snapshot(ctx, "p", "default", "writes", nil)
	if err != nil || len(data.Rows) != 1 {
		t.Fatal(data.Rows, err)
	}
}
func TestWriteFaultMetricsMeasureConcurrencyAndResetOnlyWhenIdle(t *testing.T) {
	_, h := fixture(t, Config{TestMode: true})
	putFault(t, h, "slow-create", FaultRule{Match: FaultMatch{Plane: "tunnel", Object: "upload", Action: "create", Project: "p", Table: "t"}, Effect: FaultEffect{Type: "delay", DelayMS: 100}, Times: 8})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := h.Client().Post(h.URL+"/projects/p/tables/t?uploads", "application/json", strings.NewReader("{}"))
			if err != nil {
				t.Error(err)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Error(resp.StatusCode)
			}
		}()
	}
	wg.Wait()
	code, b := sendBody(t, h, "GET", "/__test/write-metrics", nil, nil)
	var metrics map[string]int
	json.Unmarshal(b, &metrics)
	if code != 200 || metrics["active"] != 0 || metrics["peak"] < 2 {
		t.Fatal(code, metrics)
	}
	code, b = sendBody(t, h, "POST", "/__test/write-metrics", nil, nil)
	json.Unmarshal(b, &metrics)
	if code != 200 || metrics["peak"] != 0 {
		t.Fatal(code, metrics)
	}
}
func TestWriteFaultStageClassification(t *testing.T) {
	s, _ := fixture(t, Config{})
	for _, row := range []struct{ method, path, plane, object, action string }{{"POST", "/projects/p/tables/t?uploads", "tunnel", "upload", "create"}, {"PUT", "/projects/p/tables/t?uploadid=s&blockid=0", "tunnel", "upload", "write"}, {"POST", "/projects/p/tables/t/upserts?upsertid=s", "tunnel", "upsert", "commit"}, {"POST", "/api/storage/v2?Target=projects.p.schemas.default.tables.t&Action=TableCloseWriteStream", "storage", "write", "close_stream"}} {
		request, _ := http.NewRequest(row.method, row.path, nil)
		trace := s.requestTrace(request)
		if trace.Plane != row.plane || trace.Object != row.object || trace.Action != row.action {
			t.Fatalf("%#v", trace)
		}
	}
}

func TestLostCommitAcknowledgementDoesNotRepublishCommittedBlocks(t *testing.T) {
	s, h := fixture(t, Config{TestMode: true})
	ctx := context.Background()
	if _, err := s.Engine.Execute(ctx, "p", "default", "CREATE TABLE writes(id BIGINT)"); err != nil {
		t.Fatal(err)
	}
	column, _ := engine.NewColumn("id", "bigint")
	payload, err := wire.Protobuf(engine.Result{Columns: []engine.Column{column}, Rows: [][]any{{int64(7)}}})
	if err != nil {
		t.Fatal(err)
	}
	code, body := sendBody(t, h, "POST", "/projects/p/tables/writes?uploads", nil, nil)
	if code != 200 {
		t.Fatal(code, string(body))
	}
	var session map[string]any
	if err := json.Unmarshal(body, &session); err != nil {
		t.Fatal(err)
	}
	path := "/projects/p/tables/writes?uploadid=" + session["UploadID"].(string)
	code, body = sendBody(t, h, "PUT", path+"&blockid=0", payload, nil)
	if code != 200 {
		t.Fatal(code, string(body))
	}
	putFault(t, h, "lost-commit", FaultRule{Match: FaultMatch{Plane: "tunnel", Object: "upload", Action: "commit", Project: "p", Table: "writes"}, Effect: FaultEffect{Type: "lost_ack"}, Times: 1})
	request, _ := http.NewRequest("POST", h.URL+path, strings.NewReader(`{}`))
	response, err := h.Client().Do(request)
	if err == nil {
		response.Body.Close()
		t.Fatal("commit ACK was not lost")
	}
	// The original commit must already be visible before the retry.
	data, _, err := s.Engine.Snapshot(ctx, "p", "default", "writes", nil)
	if err != nil || len(data.Rows) != 1 {
		t.Fatal(data.Rows, err)
	}
	code, body = sendBody(t, h, "POST", path, []byte(`{}`), nil)
	if code != 200 {
		t.Fatal(code, string(body))
	}
	data, _, err = s.Engine.Snapshot(ctx, "p", "default", "writes", nil)
	if err != nil || len(data.Rows) != 1 {
		t.Fatal(data.Rows, err)
	}
}
