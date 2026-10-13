package server

import (
	"context"
	"encoding/json"
	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"testing"
)

func TestUpsertPartialUpdateIsTableOptIn(t *testing.T) {
	s, h := fixture(t, Config{})
	for _, enabled := range []bool{false, true} {
		name, properties := "pk_default", "tblproperties('transactional'='true','write.bucket.num'='1')"
		if enabled {
			name = "pk_partial"
			properties = "tblproperties('transactional'='true','write.bucket.num'='1','acid.partial.fields.update.enable'='true')"
		}
		if _, err := s.Engine.Execute(context.Background(), "p", "default", "create table "+name+"(id bigint,v string,primary key(id)) "+properties); err != nil {
			t.Fatal(err)
		}
		code, body := sendBody(t, h, "POST", "/projects/p/tables/"+name+"/upserts", nil, nil)
		var session map[string]any
		if err := json.Unmarshal(body, &session); err != nil {
			t.Fatal(err)
		}
		if code != 200 || session["enable_partial_update"] != enabled {
			t.Fatalf("%d %s", code, body)
		}
	}
}

func TestDeclaredUpsertBucketsAppearInSlotAssignment(t *testing.T) {
	meta := engine.Table{Properties: map[string]string{"write.bucket.num": "16"}, PrimaryKeys: []string{"id"}}
	value := writeJSON(&writeSession{Meta: meta, Kind: "upsert", Status: "normal"}, "127.0.0.1:1")
	slots := value["slots"].([]any)
	buckets := slots[0].(map[string]any)["buckets"].([]int)
	if len(buckets) != 16 || buckets[0] != 0 || buckets[15] != 15 {
		t.Fatalf("%#v", buckets)
	}
}
