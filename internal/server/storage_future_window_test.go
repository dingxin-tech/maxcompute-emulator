package server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

// TestStorageIncrementalFutureEndIsRefusedOnBothSurfaces is the public-surface half of the same
// review finding. Both v1 (session_type=incremental_read) and v2 (IncrementalRead=true with
// IncrementalReadOptions) hand the consumer LatestVersion as the version to resume from, so a
// future EndVersion that is echoed back is how a real consumer loses data.
//
// The live service refuses such a window ("The query version exceeded timetravel range", read
// off a throwaway ACID table on 2026-11-12), so the emulator refuses it too - 400 with
// InvalidParameter, the value-is-not-acceptable family, not a claimed match on the service's
// own status/code, which the public SDK does not expose.
func TestStorageIncrementalFutureEndIsRefusedOnBothSurfaces(t *testing.T) {
	s, h := fixture(t, Config{})
	createAcidTable(t, s, "acidf", 1, 2)
	body := func(from, to int64) string {
		return `{"IncrementalReadOptions":{"StartVersion":` + strconv.FormatInt(from, 10) +
			`,"EndVersion":` + strconv.FormatInt(to, 10) + `,"Mode":"cdc"}}`
	}
	cases := []struct {
		name             string
		from, to         int64
		wantRefusalChild string
	}{
		{"future end by one", 0, 3, "exceeds the committed version 2"},
		{"future end far", 0, 100, "exceeds the committed version 2"},
		{"start and end above the bound", 5, 100, "exceeds the committed version 2"},
	}
	for _, tc := range cases {
		code, resp, sid := openSession(t, h, v1Path("acidf", "/sessions?session_type=incremental_read"), body(tc.from, tc.to))
		if code != http.StatusBadRequest || sid != "" {
			t.Fatalf("v1 %s: code=%d want 400 with no session, body=%v", tc.name, code, resp)
		}
		if !strings.Contains(fmt.Sprint(resp), "InvalidParameter") || !strings.Contains(fmt.Sprint(resp), tc.wantRefusalChild) {
			t.Fatalf("v1 %s: body=%v want InvalidParameter naming %q", tc.name, resp, tc.wantRefusalChild)
		}
		code, resp2, sid2 := openSession(t, h,
			"/api/storage/v2?Action=TableCreateReadSession&Target=projects.p.schemas.default.tables.acidf",
			`{"IncrementalRead":true,"IncrementalReadOptions":{"StartVersion":`+strconv.FormatInt(tc.from, 10)+
				`,"EndVersion":`+strconv.FormatInt(tc.to, 10)+`,"Mode":"cdc"}}`)
		if code != http.StatusBadRequest || sid2 != "" {
			t.Fatalf("v2 %s: code=%d want 400 with no session, body=%v", tc.name, code, resp2)
		}
	}
	// The window the service does answer: end exactly at the bound. Nothing is consumed and no
	// cursor moves on a refused request, so versions committed afterwards stay reachable.
	code, exact, eid := openSession(t, h, v1Path("acidf", "/sessions?session_type=incremental_read"), body(0, 2))
	if code != http.StatusOK || eid == "" {
		t.Fatalf("v1 exact window: code=%d body=%v", code, exact)
	}
	if v := int64(exact["LatestVersion"].(float64)); v != 2 {
		t.Fatalf("v1 exact window LatestVersion=%d want 2", v)
	}
	insertAcidRows(t, s, "acidf", 3, 4)
	code, resumed, sid := openSession(t, h, v1Path("acidf", "/sessions?session_type=incremental_read"), body(2, 100))
	if code != http.StatusBadRequest || sid != "" {
		t.Fatalf("v1 resume with a future end: code=%d want 400 body=%v", code, resumed)
	}
	code, resumed, sid = openSession(t, h, v1Path("acidf", "/sessions?session_type=incremental_read"), body(2, -1))
	if code != http.StatusOK || sid == "" {
		t.Fatalf("v1 resume at the bound: code=%d body=%v", code, resumed)
	}
	if v := int64(resumed["RecordCount"].(float64)); v != 2 {
		t.Fatalf("v1 resume RecordCount=%d want the two versions committed after the refusals", v)
	}
	if _, ops := readEvents(t, h, "acidf", sid); len(ops) != 2 {
		t.Fatalf("v1 resume events=%v want two", ops)
	}
	// And the same numbers on the v2 surface, where the echoed window must name what was
	// consumed rather than what was asked for.
	code, v2, v2sid := openSession(t, h,
		"/api/storage/v2?Action=TableCreateReadSession&Target=projects.p.schemas.default.tables.acidf",
		`{"IncrementalRead":true,"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"cdc"}}`)
	if code != http.StatusOK || v2sid == "" {
		t.Fatalf("v2 open-ended window: code=%d body=%v", code, v2)
	}
	if v := int64(v2["LatestVersion"].(float64)); v != 4 {
		t.Fatalf("v2 LatestVersion=%d want the committed bound 4", v)
	}
	if echoed, ok := v2["IncrementalReadOptions"].(map[string]any); !ok || int64(echoed["EndVersion"].(float64)) != 4 {
		t.Fatalf("v2 echoed window=%v want the committed end 4", v2["IncrementalReadOptions"])
	}
}
