package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
)

func TestV3ResumeAndTransactionalCommitMatchService(t *testing.T) {
	s, h := fixture(t, Config{})
	ctx := context.Background()
	if _, err := s.Engine.Execute(ctx, "p", "default", "create table txn_fixture (id bigint) tblproperties ('transactional'='true')"); err != nil {
		t.Fatal(err)
	}
	base := "/api/storage/v3?Target=projects.p.schemas.default.tables.txn_fixture&WriteMode=Batch&Action="
	call := func(action string, body []byte) map[string]json.RawMessage {
		t.Helper()
		code, b := sendBody(t, h, "POST", base+action, body, nil)
		var decoded map[string]json.RawMessage
		if code != 200 {
			t.Fatalf("%s returned HTTP %d: %s", action, code, b)
		}
		if err := json.Unmarshal(b, &decoded); err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	var sessionID string
	if err := json.Unmarshal(call("TableCreateWriteSession", []byte("{}"))["SessionId"], &sessionID); err != nil || sessionID == "" {
		t.Fatalf("invalid creation response: %v", err)
	}
	col, err := engine.NewColumn("id", "bigint")
	if err != nil {
		t.Fatal(err)
	}
	for i, stream := range []string{"first", "second"} {
		descriptor := []byte(fmt.Sprintf(`{"StreamId":%q,"StreamVersion":1}`, stream))
		call("TableCreateWriteStream&SessionId="+sessionID, descriptor)
		payload, err := wire.Arrow(engine.Result{Columns: []engine.Column{col}, Rows: [][]any{{int64(i + 1)}}}, 1, false)
		if err != nil {
			t.Fatal(err)
		}
		call("TableWrite&SessionId="+sessionID+"&StreamId="+stream+"&StreamVersion=1&Count=1", payload)
		call("TableCloseWriteStream&SessionId="+sessionID, descriptor)
	}
	resumed := call("TableGetWriteSession&SessionId="+sessionID, []byte("{}"))
	var streams map[string]int64
	if len(resumed["SessionId"]) != 0 || len(resumed["SessionStatus"]) != 0 ||
		json.Unmarshal(resumed["Streams"], &streams) != nil || len(streams) != 2 || streams["first"] != 1 || streams["second"] != 1 {
		t.Fatalf("incorrect resume response fields: %v", resumed)
	}
	// The production shape commits both closed streams even with one listed.
	for i := 0; i < 2; i++ {
		call("TableCommitWriteSession&SessionId="+sessionID, []byte(`{"StreamIds":["first"],"StreamVersions":[1]}`))
	}
	result, _, err := s.Engine.Snapshot(ctx, "p", "default", "txn_fixture", nil)
	if err != nil || len(result.Rows) != 2 {
		t.Fatalf("expected both closed streams exactly once, got %v: %v", result.Rows, err)
	}
}

func TestV3InstanceReadRejectsExplicitZeroCount(t *testing.T) {
	s, h := fixture(t, Config{})
	s.mu.Lock()
	s.instances["fixture-instance"] = &instance{ID: "fixture-instance", Project: "p", Schema: "default", Status: "Success", Created: time.Now()}
	s.mu.Unlock()
	base := "/api/storage/v3?Target=projects.p.instances.fixture-instance&Action="
	code, b := sendBody(t, h, "POST", base+"InstanceCreateReadSession", []byte("{}"), nil)
	var opened struct{ DownloadID string }
	if code != http.StatusOK || json.Unmarshal(b, &opened) != nil || opened.DownloadID == "" {
		t.Fatalf("create instance reader returned %d %s", code, b)
	}
	path := base + "InstanceRead&SessionId=" + opened.DownloadID + "&Offset=0&Count="
	code, b = sendBody(t, h, "POST", path+"0", []byte("{}"), nil)
	if code != http.StatusBadRequest || !strings.Contains(string(b), "ODPS-0420411") {
		t.Fatalf("explicit zero Count returned %d %s", code, b)
	}
	code, b = sendBody(t, h, "POST", path+"1", []byte("{}"), nil)
	if code != http.StatusOK || len(b) == 0 {
		t.Fatalf("nonzero Count on the empty snapshot returned %d", code)
	}
}
