package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
)

// storageV1 implements the public table-api batch read session and data routes.
// The wire contract is distinct from action-based Storage v2; snapshots are shared.
func (s *Server) storageV1(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/storage/v1/"), "/")
	if len(parts) < 7 || parts[0] != "projects" || parts[2] != "schemas" || parts[4] != "tables" {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid table resource"))
		return
	}
	q := r.URL.Query()
	action := ""
	switch {
	case len(parts) == 7 && parts[6] == "sessions" && r.Method == "POST":
		if q.Get("session_type") != "batch_read" {
			fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("unsupported session type"))
			return
		}
		action = "TableCreateReadSession"
	case len(parts) == 8 && parts[6] == "sessions" && r.Method == "GET":
		if q.Get("session_type") != "batch_read" {
			fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("unsupported session type"))
			return
		}
		action = "TableGetReadSession"
		q.Set("SessionId", parts[7])
	case len(parts) == 7 && parts[6] == "data" && r.Method == "GET":
		action = "TableRead"
		q.Set("SessionId", q.Get("session_id"))
		for from, to := range map[string]string{"row_index": "Offset", "row_count": "Count", "split_index": "Index"} {
			if q.Has(from) {
				q.Set(to, q.Get(from))
			}
		}
	default:
		fail(w, r, 405, "UnsupportedOperation", fmt.Errorf("unsupported storage v1 operation"))
		return
	}
	q.Set("Action", action)
	q.Set("Target", "projects."+parts[1]+".schemas."+parts[3]+".tables."+parts[5])
	translated := r.Clone(r.Context())
	u := *r.URL
	u.RawQuery = q.Encode()
	translated.URL = &u
	translated.Method = "POST"
	if action == "TableRead" {
		switch r.Header.Get("Accept-Encoding") {
		case "", "ZSTD", "LZ4_FRAME":
		default:
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("unsupported compression"))
			return
		}
		maxBatchRows := int64(4096)
		if q.Has("max_batch_rows") {
			var err error
			maxBatchRows, err = strconv.ParseInt(q.Get("max_batch_rows"), 10, 64)
			if err != nil || maxBatchRows < 0 || maxBatchRows > 20000 {
				fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid batch size"))
				return
			}
		}
		if maxBatchRows == 0 {
			fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("zero batch size is not implemented"))
			return
		}
		sid := q.Get("SessionId")
		s.mu.Lock()
		session := s.storage[sid]
		s.mu.Unlock()
		if session != nil {
			session.mu.Lock()
			mode := session.SplitMode
			total := int64(len(session.Data.Rows))
			session.mu.Unlock()
			if mode == "RowOffset" {
				index, err := strconv.ParseInt(q.Get("row_index"), 10, 64)
				if err != nil || index < 0 || index >= total || !q.Has("row_count") {
					fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid row range"))
					return
				}
			} else if !q.Has("split_index") {
				fail(w, r, 400, "InvalidParameter", fmt.Errorf("missing split index"))
				return
			}
		}
		skip := int64(0)
		if q.Has("skip_row_num") {
			var err error
			skip, err = strconv.ParseInt(q.Get("skip_row_num"), 10, 64)
			if err != nil || skip < 0 {
				fail(w, r, 400, "ReadDataError", fmt.Errorf("invalid skip row count"))
				return
			}
		}
		// Resume skips records within the selected split, never beyond its range.
		if session != nil {
			session.mu.Lock()
			mode, total := session.SplitMode, uint64(len(session.Data.Rows))
			session.mu.Unlock()
			start, count := uint64(0), total
			if mode == "RowOffset" {
				start, _ = strconv.ParseUint(q.Get("row_index"), 10, 64)
				var err error
				count, err = strconv.ParseUint(q.Get("row_count"), 10, 64)
				if err != nil {
					fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid row count"))
					return
				}
				count = min(count, total-start)
			}
			skipped := min(uint64(skip), count)
			q.Set("Offset", strconv.FormatUint(start+skipped, 10))
			q.Set("Count", strconv.FormatUint(count-skipped, 10))
			translated.URL.RawQuery = q.Encode()
			skip = 0
		}
		if format := q.Get("data_format_type"); format != "" && !strings.EqualFold(format, "Arrow") {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("unsupported format"))
			return
		}
		if version := q.Get("data_format_version"); version != "" && !strings.EqualFold(version, "V5") {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("unsupported format version"))
			return
		}
		columns := splitNonEmpty(q.Get("data_columns"))
		if session != nil && len(columns) > 0 {
			session.mu.Lock()
			last := 0
			seen := map[string]bool{}
			for _, name := range columns {
				if strings.EqualFold(q.Get("data_columns_unordered"), "true") {
					last = 0
				}
				found := -1
				for i := last; i < len(session.Data.Columns); i++ {
					if strings.EqualFold(name, session.Data.Columns[i].Name) {
						found = i
						break
					}
				}
				if found < 0 || seen[strings.ToLower(name)] {
					session.mu.Unlock()
					fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid read column order or subset"))
					return
				}
				seen[strings.ToLower(name)] = true
				last = found
			}
			session.mu.Unlock()
		}
		body, _ := json.Marshal(map[string]any{"MaxBatchRows": maxBatchRows, "SkipRowNum": skip, "DataColumns": columns})
		translated.Body = io.NopCloser(bytes.NewReader(body))
		s.storageAPI(w, translated)
		return
	}
	rec := httptest.NewRecorder()
	s.storageAPI(rec, translated)
	if rec.Code != http.StatusOK {
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		w.Write(rec.Body.Bytes())
		return
	}
	var result map[string]any
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&result); err != nil {
		fail(w, r, 500, "InvalidData", err)
		return
	}
	result["SessionType"] = "batch_read"
	result["SessionStats"] = map[string]any{"EstimatedSize": -1, "EstimatedRowCount": result["RecordCount"]}
	jsonResponse(w, http.StatusOK, result)
}

func splitNonEmpty(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}
