package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
)

func TestWriteSessionsRejectRecreatedTable(t *testing.T) {
	for _, kind := range []string{"batch", "stream", "storage"} {
		t.Run(kind, func(t *testing.T) {
			s, h := fixture(t, Config{})
			ctx := context.Background()
			if _, err := s.Engine.Execute(ctx, "p", "default", "create table target(id bigint)"); err != nil {
				t.Fatal(err)
			}
			c, _ := engine.NewColumn("id", "bigint")
			data := engine.Result{Columns: []engine.Column{c}, Rows: [][]any{{int64(7)}}}
			var path string
			var payload []byte
			var m map[string]any
			if kind == "storage" {
				base := "/api/storage/v2?Target=projects.p.schemas.default.tables.target&Action="
				code, b := sendBody(t, h, "POST", base+"TableCreateWriteSession", []byte(`{}`), nil)
				if code != 200 {
					t.Fatal(code, string(b))
				}
				json.Unmarshal(b, &m)
				sid := m["SessionId"].(string)
				code, b = sendBody(t, h, "POST", base+"TableCreateWriteStream&SessionId="+sid, []byte(`{"StreamId":"s","StreamVersion":1}`), nil)
				if code != 200 {
					t.Fatal(code, string(b))
				}
				payload, _ = wire.Arrow(data, 1, false)
				code, b = sendBody(t, h, "POST", base+"TableWrite&SessionId="+sid+"&StreamId=s&StreamVersion=1&Count=1", payload, nil)
				if code != 200 {
					t.Fatal(code, string(b))
				}
				code, b = sendBody(t, h, "POST", base+"TableCloseWriteStream&SessionId="+sid, []byte(`{"StreamId":"s","StreamVersion":1}`), nil)
				if code != 200 {
					t.Fatal(code, string(b))
				}
				path = base + "TableCommitWriteSession&SessionId=" + sid
			} else {
				endpoint := "/projects/p/tables/target"
				create := endpoint + "?uploads"
				idKey := "UploadID"
				if kind == "stream" {
					endpoint += "/streams"
					create = endpoint
					idKey = "session_name"
				}
				code, b := sendBody(t, h, "POST", create, nil, nil)
				if code != 200 {
					t.Fatal(code, string(b))
				}
				json.Unmarshal(b, &m)
				path = endpoint + "?uploadid=" + m[idKey].(string)
				payload, _ = wire.Protobuf(data)
				if kind == "batch" {
					code, b = sendBody(t, h, "PUT", path+"&blockid=0", payload, nil)
					if code != 200 {
						t.Fatal(code, string(b))
					}
				}
			}
			if _, err := s.Engine.Execute(ctx, "p", "default", "drop table target; create table target(other string)"); err != nil {
				t.Fatal(err)
			}
			method := "POST"
			body := []byte(`{}`)
			if kind == "stream" {
				method = "PUT"
				body = payload
			}
			code, b := sendBody(t, h, method, path, body, nil)
			if code < 400 || code >= 500 {
				t.Fatalf("stale %s accepted or internal error: %d %s", kind, code, b)
			}
			result, err := s.Engine.Execute(ctx, "p", "default", "select * from target")
			if err != nil || len(result.Rows) != 0 {
				t.Fatalf("new table was mutated: %v %v", result.Rows, err)
			}
		})
	}
}

func TestBatchBlockReplacementAtCapacity(t *testing.T) {
	s, h := fixture(t, Config{})
	if _, err := s.Engine.Execute(context.Background(), "p", "default", "create table target(id bigint)"); err != nil {
		t.Fatal(err)
	}
	code, b := sendBody(t, h, "POST", "/projects/p/tables/target?uploads", nil, nil)
	if code != 200 {
		t.Fatal(code, string(b))
	}
	var m map[string]any
	json.Unmarshal(b, &m)
	sid := m["UploadID"].(string)
	// Pre-fill distinct empty blocks to isolate the capacity boundary without 1024 HTTP writes.
	v := s.writes[sid]
	for i := int64(0); i < 1024; i++ {
		v.Blocks[i] = engine.Result{}
		v.BlockCounts[i] = 0
	}
	c, _ := engine.NewColumn("id", "bigint")
	payload, _ := wire.Protobuf(engine.Result{Columns: []engine.Column{c}, Rows: [][]any{{int64(7)}}})
	path := "/projects/p/tables/target?uploadid=" + sid + "&blockid="
	code, b = sendBody(t, h, "PUT", path+"0", payload, nil)
	if code != 200 {
		t.Fatal("replacement rejected", code, string(b))
	}
	code, _ = sendBody(t, h, "PUT", path+"1024", payload, nil)
	if code != 429 {
		t.Fatal("new block exceeded capacity", code)
	}
	code, b = sendBody(t, h, "POST", "/projects/p/tables/target?uploadid="+sid, nil, nil)
	if code != 200 {
		t.Fatal(code, string(b))
	}
	result, err := s.Engine.Execute(context.Background(), "p", "default", "select * from target")
	if err != nil || len(result.Rows) != 1 {
		t.Fatal(result.Rows, err)
	}
}
