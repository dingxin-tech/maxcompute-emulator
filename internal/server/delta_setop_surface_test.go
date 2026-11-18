package server

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// sqlInstance runs one statement through the SQL plane exactly the way a client does: POST the
// instance, follow the returned Location, and report the task status and result text.
func sqlInstance(t *testing.T, h *httptest.Server, query string) (string, string) {
	t.Helper()
	post := `<Instance><Job><Tasks><SQL><Name>setop</Name><Query>` + query + `</Query></SQL></Tasks></Job></Instance>`
	resp, err := h.Client().Post(h.URL+"/projects/p/instances", "application/xml", strings.NewReader(post))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return "submit-" + resp.Status, string(raw)
	}
	loc := resp.Header.Get("Location")
	if loc == "" {
		t.Fatalf("no Location for %s: %s", query, raw)
	}
	g, err := h.Client().Get(h.URL + loc)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Body.Close()
	body, _ := io.ReadAll(g.Body)
	var out struct {
		Status string `xml:"Status"`
		Tasks  struct {
			Task struct {
				Status string `xml:"Status"`
				Result string `xml:"Result"`
			} `xml:"Task"`
		} `xml:"Tasks"`
	}
	if err = xml.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode %s: %v %s", loc, err, body)
	}
	return out.Tasks.Task.Status, out.Tasks.Task.Result
}

// TestSQLPlaneStarCopyAcrossSetOperations pins the review's two cells on the plane a consumer
// actually drives. UNION over two delta tables holding the same user row at different versions
// must answer one row: the stage runs the whole source in one statement, so if the star is not
// projected before the relations meet, the set operation deduplicates on the hidden version
// columns. A star that cannot be attributed to a single relation is refused on the same plane
// instead of staging rows nobody declared.
func TestSQLPlaneStarCopyAcrossSetOperations(t *testing.T) {
	s, h := fixture(t, Config{})
	ctx := context.Background()
	for _, q := range []string{
		"create table sa(id bigint, s string, primary key(id)) tblproperties('transactional'='true')",
		"create table sb(id bigint, s string, primary key(id)) tblproperties('transactional'='true')",
		"create table su(id bigint, s string)",
		"create table sj(id bigint, s string)",
		"insert into sa values (1,'x')",
		"insert into sb values (1,'x')",
		"truncate table sb",
		"insert into sb values (1,'x')",
	} {
		if _, err := s.Engine.Execute(ctx, "p", "default", q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if status, result := sqlInstance(t, h, "insert into su select * from sa union select * from sb"); status != "Success" {
		t.Fatalf("SQL plane UNION copy: status=%s result=%s want Success", status, result)
	}
	rows, err := s.Engine.Execute(ctx, "p", "default", "select id, s from su")
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("su rows=%d err=%v want the single user row", len(rows.Rows), err)
	}
	// 事件面：这一格落到 delta 目标时，历史里只有一条插入
	if _, err = s.Engine.Execute(ctx, "p", "default",
		"create table sd(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"); err != nil {
		t.Fatal(err)
	}
	if status, result := sqlInstance(t, h, "insert into sd select * from sa union select * from sb"); status != "Success" {
		t.Fatalf("SQL plane UNION into a delta target: status=%s result=%s", status, result)
	}
	ev, cursor, err := s.Engine.DeltaRead(ctx, "p", "default", "sd", nil, 0, -1, true)
	if err != nil || len(ev.Rows) != 1 {
		t.Fatalf("sd events=%d err=%v want one insert", len(ev.Rows), err)
	}
	if _, err = s.Engine.Execute(ctx, "p", "default", "insert into sd values (2,'y')"); err != nil {
		t.Fatal(err)
	}
	tail, advanced, err := s.Engine.DeltaRead(ctx, "p", "default", "sd", nil, cursor, -1, true)
	if err != nil || len(tail.Rows) != 1 || advanced <= cursor {
		t.Fatalf("resume after the copy rows=%d cursor=%d->%d err=%v", len(tail.Rows), cursor, advanced, err)
	}
	// 归不到单一关系的星号：实例失败，且没有任何行被暂存
	status, result := sqlInstance(t, h, "insert into sj select * from sa join sb on sa.id=sb.id union select * from su")
	if status != "Failed" || !strings.Contains(result, "UnsupportedFeature") {
		t.Fatalf("star over a join: status=%s result=%s want Failed with UnsupportedFeature", status, result)
	}
	if rows, err = s.Engine.Execute(ctx, "p", "default", "select id, s from sj"); err != nil || len(rows.Rows) != 0 {
		t.Fatalf("a refused source must stage nothing: rows=%d err=%v", len(rows.Rows), err)
	}
	// 没有 delta 参与的 plain UNION 保持原样
	if status, result = sqlInstance(t, h, "insert into sj select id, s from su union select id, s from su"); status != "Success" {
		t.Fatalf("plain-only union: status=%s result=%s want Success", status, result)
	}
	if rows, err = s.Engine.Execute(ctx, "p", "default", "select id, s from sj"); err != nil || len(rows.Rows) != 1 {
		t.Fatalf("plain-only union rows=%d err=%v want one row", len(rows.Rows), err)
	}
}
