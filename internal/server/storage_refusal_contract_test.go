package server

import (
	"context"
	"fmt"
	"net/http"
	"testing"
)

// TestStorageRefusalContract pins the storage-face refusal rule the maintainer asked to
// be unified on 2026-11-04: a capability the emulator does not provide answers
// 405/MethodNotAllowed, while a value that is malformed, out of range or unknown answers
// 400/InvalidParameter. Before that decision the first group answered 400 with
// UnsupportedOperation or InvalidParameter, so both classes looked identical on the wire -
// which is exactly what a consumer's retry logic must not confuse. The table keeps one
// example per site in both directions, so a future change cannot collapse the classes again.
func TestStorageRefusalContract(t *testing.T) {
	s, h := fixture(t, Config{})
	ctx := context.Background()
	if _, err := s.Engine.Execute(ctx, "p", "default", "create table plain(id bigint, s string)"); err != nil {
		t.Fatal(err)
	}
	createAcidTable(t, s, "acidc", 1, 2)
	data, _, sid := openSession(t, h, v1Path("acidc", "/sessions?session_type=incremental_read"),
		`{"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"append"}}`)
	if data != http.StatusOK || sid == "" {
		t.Fatalf("control: a supported incremental session must be created, got %d %v", data, sid)
	}
	// v2 answers a refusal with an XML/JSON body carrying <Code>; fmt.Sprint keeps a
	// successful response (no Code key) as "<nil>" instead of panicking, so a wrong
	// expectation reads as a failed comparison rather than as a crash.
	v2 := func(action, body string) (int, string) {
		code, out, _ := openSession(t, h, "/api/storage/v2?Action="+action+"&Target=projects.p.schemas.default.tables.t", body)
		return code, fmt.Sprint(out["Code"])
	}
	cases := []struct {
		name, wantCode string
		want           int
		get            func() int
	}{
		// capability the emulator does not provide -> 405
		{"v1 unsupported compression", "MethodNotAllowed", http.StatusMethodNotAllowed, func() int {
			code, _, _ := request(t, h, "GET", v1Path("t", "/data?session_id=x&row_index=0&row_count=1"), "gzip")
			return code
		}},
		{"v1 unsupported format", "MethodNotAllowed", http.StatusMethodNotAllowed, func() int {
			code, _, _ := request(t, h, "GET", v1Path("t", "/data?session_id=x&row_index=0&row_count=1&data_format_type=protobuf"), "ZSTD")
			return code
		}},
		{"v1 unsupported format version", "MethodNotAllowed", http.StatusMethodNotAllowed, func() int {
			code, _, _ := request(t, h, "GET", v1Path("t", "/data?session_id=x&row_index=0&row_count=1&data_format_version=v4"), "ZSTD")
			return code
		}},
		{"v1 zero batch size", "MethodNotAllowed", http.StatusMethodNotAllowed, func() int {
			code, _, _ := request(t, h, "GET", v1Path("t", "/data?session_id=x&row_index=0&max_batch_rows=0"), "ZSTD")
			return code
		}},
		// The public SDK sends WriteMode as a query parameter, which is what the server reads.
		{"v2 unsupported write mode", "MethodNotAllowed", http.StatusMethodNotAllowed, func() int {
			code, _, _ := openSession(t, h, "/api/storage/v2?Action=TableCreateWriteSession&WriteMode=Magic&Target=projects.p.schemas.default.tables.t", `{}`)
			return code
		}},
		{"v2 filter pushdown", "MethodNotAllowed", http.StatusMethodNotAllowed, func() int {
			code, _ := v2("TableCreateReadSession", `{"FilterPredicate":"id=1"}`)
			return code
		}},
		{"v2 bucket split", "MethodNotAllowed", http.StatusMethodNotAllowed, func() int {
			code, _ := v2("TableCreateReadSession", `{"SplitOptions":{"SplitMode":"Bucket"}}`)
			return code
		}},
		{"v2 bucket sort", "MethodNotAllowed", http.StatusMethodNotAllowed, func() int {
			code, _ := v2("TableCreateReadSession", `{"SplitOptions":{"SplitMode":"Size","BucketSort":true}}`)
			return code
		}},
		{"incremental on a plain table", "MethodNotAllowed", http.StatusMethodNotAllowed, func() int {
			code, _, _ := openSession(t, h, v1Path("plain", "/sessions?session_type=incremental_read"),
				`{"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"append"}}`)
			return code
		}},
		{"incremental timestamp window", "MethodNotAllowed", http.StatusMethodNotAllowed, func() int {
			code, _, _ := openSession(t, h, v1Path("acidc", "/sessions?session_type=incremental_read"),
				`{"IncrementalReadOptions":{"StartTimeStamp":"2026-11-04 00:00:00","Mode":"append"}}`)
			return code
		}},
		{"incremental transaction id", "MethodNotAllowed", http.StatusMethodNotAllowed, func() int {
			code, _, _ := openSession(t, h, v1Path("acidc", "/sessions?session_type=incremental_read"),
				`{"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"append","StartTxnId":"7"}}`)
			return code
		}},
		// a value that is not acceptable -> 400, unchanged by the unification
		{"v1 out of range batch size", "InvalidParameter", http.StatusBadRequest, func() int {
			code, _, _ := request(t, h, "GET", v1Path("t", "/data?session_id=x&row_index=0&max_batch_rows=20001"), "ZSTD")
			return code
		}},
		{"v1 unknown session type", "InvalidParameter", http.StatusBadRequest, func() int {
			code, _, _ := openSession(t, h, v1Path("t", "/sessions?session_type=delta_read"), `{}`)
			return code
		}},
		{"v2 unknown split mode", "InvalidParameter", http.StatusBadRequest, func() int {
			code, _ := v2("TableCreateReadSession", `{"SplitOptions":{"SplitMode":"Magic"}}`)
			return code
		}},
		{"incremental unknown mode", "InvalidParameter", http.StatusBadRequest, func() int {
			code, _, _ := openSession(t, h, v1Path("acidc", "/sessions?session_type=incremental_read"),
				`{"IncrementalReadOptions":{"StartVersion":0,"EndVersion":-1,"Mode":"merge"}}`)
			return code
		}},
		{"incremental inverted window", "InvalidParameter", http.StatusBadRequest, func() int {
			code, _, _ := openSession(t, h, v1Path("acidc", "/sessions?session_type=incremental_read"),
				`{"IncrementalReadOptions":{"StartVersion":9,"EndVersion":3,"Mode":"append"}}`)
			return code
		}},
		// A window ending above the committed version is a value the service refuses too
		// (measured 2026-11-12), so it belongs to this family rather than to 405.
		{"incremental end past the bound", "InvalidParameter", http.StatusBadRequest, func() int {
			code, _, _ := openSession(t, h, v1Path("acidc", "/sessions?session_type=incremental_read"),
				`{"IncrementalReadOptions":{"StartVersion":0,"EndVersion":100,"Mode":"append"}}`)
			return code
		}},
	}
	for _, tc := range cases {
		if got := tc.get(); got != tc.want {
			t.Fatalf("%s: status=%d want %d", tc.name, got, tc.want)
		}
	}
	if _, code := v2("TableCreateReadSession", `{"FilterPredicate":"id=1"}`); code != "MethodNotAllowed" {
		t.Fatalf("refusal body code=%q want MethodNotAllowed", code)
	}
}
