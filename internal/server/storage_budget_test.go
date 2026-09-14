package server

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
)

func TestStorageRetryBudgetRejectsNewWriteButAllowsReplay(t *testing.T) {
	s, h := fixture(t, Config{})
	meta, err := s.Engine.Table(context.Background(), "p", "default", "t")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := wire.Arrow(engine.Result{Columns: meta.Columns, Rows: [][]any{{int64(9999), "new"}}}, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	st := &storageStream{ID: "stream", Version: 1, Exactly: true, Offset: 10000, Requests: map[int64][32]byte{}}
	for n := int64(0); n < 10000; n++ {
		st.Requests[n] = sha256.Sum256(payload)
	}
	s.storage["session"] = &storageSession{ID: "session", Target: "projects.p.schemas.default.tables.t", Project: "p", Schema: "default", Table: "t", Meta: meta, Mode: "Streaming", Status: "NORMAL", Created: time.Now(), Streams: map[string]*storageStream{"stream": st}}
	path := "/api/storage/v2?Target=projects.p.schemas.default.tables.t&WriteMode=Streaming&Action=TableWrite&SessionId=session&StreamId=stream&StreamVersion=1&Count=1&RowOffset="
	code, body := sendBody(t, h, "POST", path+"10000", payload, nil)
	if code != 429 {
		t.Fatalf("new request bypassed retry budget: %d %s", code, body)
	}
	code, body = sendBody(t, h, "POST", path+"0", payload, nil)
	if code != 200 {
		t.Fatalf("existing retry rejected at capacity: %d %s", code, body)
	}
	result, err := s.Engine.Execute(context.Background(), "p", "default", "select count(*) from t where id=9999")
	if err != nil || wire.Int(result.Rows[0][0]) != 0 {
		t.Fatalf("rejected write became visible: %v %v", result.Rows, err)
	}
}

func TestEmptyUpsertPacksCannotGrowStagingWithoutLimit(t *testing.T) {
	s, h := fixture(t, Config{})
	meta, err := s.Engine.Table(context.Background(), "p", "default", "t")
	if err != nil {
		t.Fatal(err)
	}
	v := &writeSession{ID: "upsert", Project: "p", Schema: "default", Table: "t", Partition: "{}", Kind: "upsert", Status: "normal", Meta: meta, Created: time.Now(), Sequence: 1024, Blocks: map[int64]engine.Result{}, BlockCounts: map[int64]int{}}
	for n := int64(0); n < 1024; n++ {
		v.Blocks[n] = engine.Result{}
	}
	s.writes[v.ID] = v
	payload, err := wire.Protobuf(engine.Result{})
	if err != nil {
		t.Fatal(err)
	}
	code, body := sendBody(t, h, "PUT", "/projects/p/tables/t/upserts?upsertid=upsert", payload, nil)
	if code != 429 {
		t.Fatalf("empty upsert bypassed staging limit: %d %s", code, body)
	}
}
