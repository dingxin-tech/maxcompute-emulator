package server

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// submitStatementWithSettings is submitStatement plus the per-statement settings block,
// which is where a client says it wants non-select statements to run in the session.
func submitStatementWithSettings(t *testing.T, h *httptest.Server, id, query string, settings map[string]string) (int, informationResult) {
	t.Helper()
	payload, e := json.Marshal(map[string]any{"query": query, "settings": settings})
	if e != nil {
		t.Fatal(e)
	}
	body := "<Instance><Key>query</Key><Value>" + string(payload) + "</Value></Instance>"
	code, _, b := mcqaCall(t, h, "PUT", infoPath(id, sessionTask, ""), []byte(body))
	if code != 200 {
		t.Fatalf("setInformation code=%d body=%s", code, b)
	}
	var ir informationResult
	if e := json.Unmarshal(b, &ir); e != nil {
		t.Fatalf("setInformation body not JSON: %v / %s", e, b)
	}
	return parseQueryID(t, ir), ir
}

func refusedResult(t *testing.T, ir informationResult) map[string]any {
	t.Helper()
	var sub map[string]any
	if e := json.Unmarshal([]byte(ir.Result), &sub); e != nil {
		t.Fatalf("sub query payload is not an object: %v / %s", e, ir.Result)
	}
	return sub
}

func TestIsSelectStatement(t *testing.T) {
	cases := []struct {
		sql  string
		want bool
	}{
		{"select 1", true},
		{"  SELECT id FROM t", true},
		{"with x as (select 1 as a) select * from x", true},
		{"(select 1)", true},
		{"-- a leading comment\nselect 1", true},
		{"/* hint */ select 1", true},
		{"create table t(a int)", false},
		{"insert into t values (1)", false},
		{"truncate table t", false},
		{"select 1; insert into t values (1)", false},
		{"select ';' as x", true},
		{"", false},
		{"   ", false},
	}
	for _, c := range cases {
		if got := isSelectStatement(c.sql); got != c.want {
			t.Errorf("isSelectStatement(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

// TestSessionRefusesNonSelectWithoutRunningIt is the point of the file: the refusal has to
// come before execution, because the client's reaction to a refusal is to run the same
// statement again somewhere else.
func TestSessionRefusesNonSelectWithoutRunningIt(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	subQueryID, ir := submitStatementWithSettings(t, h, id, "create table nope(a bigint)", nil)
	if ir.Status != mcqaInfoOK || subQueryID != -1 {
		t.Fatalf("a select-only session must decline to start a DDL: %+v", ir)
	}
	result := fmt.Sprint(refusedResult(t, ir)["result"])
	for _, want := range []string{"ODPS-185", "Non select query not supported"} {
		if !strings.Contains(result, want) {
			t.Fatalf("refusal %q must carry %q: the first is what makes the client retry offline, the second is what a human reads", result, want)
		}
	}
	if code, _, b := request(t, h, "GET", "/projects/p/tables/nope", ""); code == 200 {
		t.Fatalf("the refused statement must not have run: table exists, body=%s", b)
	}
	// Nothing was allocated, so the next statement is still the session's first.
	if next, _ := submitStatementWithSettings(t, h, id, "select 1", nil); next != 1 {
		t.Fatalf("a declined submission must not consume a sub query id, got %d", next)
	}
}

// TestSessionRunsNonSelectWhenTheClientSaysSo covers the other half: the restriction is a
// default, not an opinion of the emulator's.
func TestSessionRunsNonSelectWhenTheClientSaysSo(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, "")
	subQueryID, ir := submitStatementWithSettings(t, h, id, "create table yes(a bigint)",
		map[string]string{mcqaSelectOnlyKey: "false"})
	if ir.Status != mcqaInfoOK || subQueryID < 1 {
		t.Fatalf("with %s=false the session must run the statement: %+v", mcqaSelectOnlyKey, ir)
	}
	if code, _, b := request(t, h, "GET", "/projects/p/tables/yes", ""); code != 200 {
		t.Fatalf("the statement should have run inside the session: code=%d body=%s", code, b)
	}
}

func TestSessionCreateSettingLiftsSelectOnlyForEveryStatement(t *testing.T) {
	_, h := fixture(t, Config{SessionTTL: time.Hour})
	id := createSession(t, h, `{"`+mcqaSelectOnlyKey+`":"false","odps.sql.session.name":"wide"}`)
	if subQueryID, _ := submitStatementWithSettings(t, h, id, "create table wide_session(a bigint)", nil); subQueryID != 1 {
		t.Fatalf("the session-level setting should apply without a per-statement one, got id=%d", subQueryID)
	}
}

func TestSQLStatementsKeepsSemicolonsInsideStrings(t *testing.T) {
	got := sqlStatements("select 'a;b' as x; -- trailing\n insert into t values (1)")
	if len(got) != 2 || !strings.HasPrefix(got[0], "select 'a;b'") || !strings.HasPrefix(got[1], "insert") {
		t.Fatalf("statements=%q", got)
	}
}
