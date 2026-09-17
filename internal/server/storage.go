package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
)

type storageStream struct {
	Token           string
	Hash            [32]byte
	Bytes           int64
	ID              string
	Version         int64
	Closed, Exactly bool
	Rows            [][]any
	Offset          int64
	Requests        map[int64][32]byte
}
type storageSession struct {
	Overwrite                                                   bool
	mu                                                          sync.Mutex
	ID, Target, Project, Schema, Table, Mode, Status, SplitMode string
	Created                                                     time.Time
	Meta                                                        engine.Table
	Data                                                        engine.Result
	Part                                                        map[string]string
	Streams                                                     map[string]*storageStream
	Bytes                                                       int64
}
type storageRequest struct {
	PartitionSpec                                                                           string
	Overwrite                                                                               bool
	CommitMessages                                                                          []string
	PartialPartitionSpec                                                                    string
	Flags                                                                                   map[string]string
	RequiredDataColumns, RequiredPartitionColumns, RequiredPartitions, DataColumns, Columns []string
	FilterPredicate                                                                         string
	IncrementalRead                                                                         bool
	SplitOptions                                                                            struct {
		SplitMode   string
		SplitNumber int64
	}
	StreamId                        string
	StreamVersion                   int64
	ExactlyOnceMode                 bool
	StreamIds                       []string
	StreamVersions                  []int64
	MaxBatchRows, SkipRowNum, Limit int64
	Partition                       string
}

func (s *Server) storageAPI(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	action, target := q.Get("Action"), q.Get("Target")
	bad := func(status int, code string, err error) { fail(w, r, status, code, err) }
	if r.Method != "POST" && !(r.Method == "GET" && action == "InstanceGetReadSession") {
		bad(405, "InvalidMethod", fmt.Errorf("expected POST"))
		return
	}
	parts := strings.Split(target, ".")
	p, sc, t := "", "default", ""
	if len(parts) == 6 && parts[0] == "projects" && parts[2] == "schemas" && parts[4] == "tables" {
		p, sc, t = parts[1], parts[3], parts[5]
	} else if len(parts) == 4 && parts[0] == "projects" && parts[2] == "instances" {
		if !s.checkProject(w, r, parts[1]) {
			return
		}
		s.storageInstance(w, r, parts[1], parts[3])
		return
	} else {
		bad(400, "InvalidParameter", fmt.Errorf("invalid Target"))
		return
	}
	if !s.checkProject(w, r, p) {
		return
	}
	req := storageRequest{}
	if action != "TableWrite" {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			bad(400, "InvalidParameter", err)
			return
		}
	}
	sid := q.Get("SessionId")
	mode := q.Get("WriteMode")
	if mode == "" {
		mode = "Batch"
	}
	if mode != "Batch" && mode != "BatchCompatible" && mode != "Streaming" && mode != "StreamingRealtime" {
		bad(400, "UnsupportedOperation", fmt.Errorf("unsupported WriteMode %s", mode))
		return
	}
	key := sid
	if sid == "default" {
		key = target + "/" + mode + "/default"
	}
	create := action == "TableCreateReadSession" || action == "TableCreateWriteSession" || action == "TablePreview"
	s.mu.Lock()
	v := s.storage[key]
	s.mu.Unlock()
	if sid == "default" && strings.HasPrefix(mode, "Streaming") && action == "TableCreateWriteStream" && v == nil {
		create = true
	}
	if create {
		meta, err := s.Engine.Table(r.Context(), p, sc, t)
		if err != nil {
			bad(404, "NoSuchTable", err)
			return
		}
		if mode == "BatchCompatible" {
			req.PartialPartitionSpec = req.PartitionSpec
			if req.Flags == nil {
				req.Flags = map[string]string{}
			}
			req.Flags["overwrite"] = strconv.FormatBool(req.Overwrite)
		}
		part, err := engine.ParsePartition(req.PartialPartitionSpec)
		if err != nil {
			bad(400, "InvalidPartition", err)
			return
		}
		v = &storageSession{ID: id(), Target: target, Project: p, Schema: sc, Table: t, Mode: mode, Status: "NORMAL", Created: time.Now(), Meta: meta, Part: part, Overwrite: req.Flags["overwrite"] == "true", Streams: map[string]*storageStream{}, SplitMode: "Size"}
		if sid == "default" {
			v.ID = sid
		} else {
			key = v.ID
		}
		if action == "TableCreateReadSession" || action == "TablePreview" {
			if req.IncrementalRead || req.FilterPredicate != "" {
				bad(400, "UnsupportedOperation", fmt.Errorf("incremental/filter reads are not implemented"))
				return
			}
			filters := []map[string]string{}
			if action == "TablePreview" && req.Partition != "" {
				req.RequiredPartitions = []string{req.Partition}
			}
			for _, ps := range req.RequiredPartitions {
				part, err := engine.ParsePartition(ps)
				if err != nil {
					bad(400, "InvalidPartition", err)
					return
				}
				filters = append(filters, part)
			}
			v.Data, v.Meta, err = s.Engine.SnapshotStorage(r.Context(), p, sc, t, filters)
			if err != nil {
				bad(400, "InvalidParameter", err)
				return
			}
			names := req.RequiredDataColumns
			if len(names) == 0 {
				for _, c := range meta.Columns {
					names = append(names, c.Name)
				}
			}
			names = append(names, req.RequiredPartitionColumns...)
			v.Data, err = projectResult(v.Data, names)
			if err != nil {
				bad(400, "InvalidParameter", err)
				return
			}
			v.Mode = "Read"
			if req.SplitOptions.SplitMode != "" {
				v.SplitMode = req.SplitOptions.SplitMode
			}
			if v.SplitMode != "Size" && v.SplitMode != "RowOffset" && v.SplitMode != "Parallelism" && v.SplitMode != "Bucket" {
				bad(400, "InvalidParameter", fmt.Errorf("unknown split mode"))
				return
			}
		}
		if action != "TablePreview" {
			s.mu.Lock()
			for k, old := range s.storage {
				if time.Since(old.Created) > s.cfg.SessionTTL {
					delete(s.storage, k)
				}
			}
			if len(s.storage) >= s.cfg.MaxSessions {
				s.mu.Unlock()
				bad(429, "ResourceLimit", fmt.Errorf("storage session limit"))
				return
			}
			if old := s.storage[key]; old != nil {
				v = old
			} else {
				s.storage[key] = v
			}
			s.mu.Unlock()
		}
	}
	if v == nil || v.Target != target || time.Since(v.Created) > s.cfg.SessionTTL {
		bad(404, "NoSuchSession", fmt.Errorf("unknown or expired session"))
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if (strings.Contains(action, "Write") || action == "TableWrite") && (v.Mode == "Read" || v.Mode != mode) {
		bad(409, "InvalidState", fmt.Errorf("session mode mismatch"))
		return
	}
	if mode == "BatchCompatible" {
		s.storageBlocks(w, r, v, req)
		return
	}
	switch action {
	case "TableCreateReadSession", "TableGetReadSession":
		if v.Mode != "Read" {
			bad(409, "InvalidState", fmt.Errorf("not a read session"))
			return
		}
		jsonResponse(w, 200, map[string]any{"SessionId": v.ID, "ExpirationTime": v.Created.Add(s.cfg.SessionTTL).Unix(), "RecordCount": len(v.Data.Rows), "SessionStatus": "NORMAL", "SessionType": "Read", "DataSchema": readSchema(v.Data.Columns), "TableSchema": writeSchema(v.Data.Columns), "SplitMode": v.SplitMode, "SplitsCount": 1, "SplitBucketId": []int{0}, "SupportedDataFormat": []any{map[string]string{"Type": "Arrow", "Version": "V5"}}, "SessionStats": map[string]any{}})
	case "TableRead", "TablePreview":
		if v.Mode != "Read" {
			bad(409, "InvalidState", fmt.Errorf("not a read session"))
			return
		}
		offset, count := int64(0), int64(len(v.Data.Rows))
		var err error
		if q.Has("Offset") {
			offset, err = strconv.ParseInt(q.Get("Offset"), 10, 64)
		}
		if err == nil && q.Has("Count") {
			count, err = strconv.ParseInt(q.Get("Count"), 10, 64)
		}
		if q.Has("Index") && q.Get("Index") != "0" {
			err = fmt.Errorf("unknown split")
		}
		offset += req.SkipRowNum
		if action == "TablePreview" {
			if req.Limit > 0 {
				count = req.Limit
			}
			req.DataColumns = req.Columns
		}
		if err != nil || offset < 0 || count < 0 || offset > int64(len(v.Data.Rows)) {
			bad(400, "InvalidParameter", fmt.Errorf("invalid row range"))
			return
		}
		count = min(count, int64(len(v.Data.Rows))-offset)
		data := v.Data
		data.Rows = data.Rows[offset : offset+count]
		data, err = projectResult(data, req.DataColumns)
		if err != nil {
			bad(400, "InvalidParameter", err)
			return
		}
		n := int(req.MaxBatchRows)
		if n <= 0 || n > 65536 {
			n = 4096
		}
		b, err := wire.Arrow(data, n, false)
		if err != nil {
			bad(400, "InvalidData", err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(b)
	case "TableCreateWriteSession", "TableGetWriteSession":
		streams := map[string]int64{}
		for k, st := range v.Streams {
			streams[k] = st.Version
		}
		jsonResponse(w, 200, map[string]any{"SessionId": v.ID, "SessionStatus": v.Status, "Streams": streams, "MinUncommittedStagingId": "0"})
	case "TableAbortWriteSession":
		if v.Status == "COMMITTED" {
			bad(409, "InvalidState", fmt.Errorf("already committed"))
			return
		}
		v.Status = "ABORTED"
		v.Streams = map[string]*storageStream{}
		v.Bytes = 0
		jsonResponse(w, 200, map[string]any{})
	case "TableCommitWriteSession":
		if v.Status == "ABORTED" {
			bad(409, "InvalidState", fmt.Errorf("aborted"))
			return
		}
		if v.Status != "COMMITTED" {
			keys := req.StreamIds
			if keys == nil {
				for k, st := range v.Streams {
					if !st.Closed {
						bad(409, "InvalidState", fmt.Errorf("unclosed stream %s", k))
						return
					}
					keys = append(keys, k)
				}
				sort.Strings(keys)
			}
			rows := [][]any{}
			seen := map[string]bool{}
			for i, k := range keys {
				st := v.Streams[k]
				if st == nil || !st.Closed || seen[k] || (len(req.StreamVersions) > 0 && (i >= len(req.StreamVersions) || req.StreamVersions[i] != st.Version)) {
					bad(409, "InvalidState", fmt.Errorf("invalid or unclosed stream %s", k))
					return
				}
				seen[k] = true
				rows = append(rows, st.Rows...)
			}
			if err := s.writeStorage(r.Context(), v, rows, v.Overwrite); err != nil {
				bad(400, "InvalidData", err)
				return
			}
			v.Status = "COMMITTED"
			v.Bytes = 0
			for _, st := range v.Streams {
				st.Rows = nil
			}
		}
		jsonResponse(w, 200, map[string]any{})
	case "TableCreateWriteStream", "TableGetWriteStream", "TableCloseWriteStream", "TableWrite":
		if v.Mode == "Read" || v.Mode != mode || v.Status != "NORMAL" {
			bad(409, "InvalidState", fmt.Errorf("write session is not open"))
			return
		}
		streamID, version := req.StreamId, req.StreamVersion
		if action == "TableWrite" || action == "TableGetWriteStream" {
			streamID = q.Get("StreamId")
			var err error
			version, err = strconv.ParseInt(q.Get("StreamVersion"), 10, 64)
			if err != nil {
				bad(400, "InvalidParameter", err)
				return
			}
		}
		if streamID == "" || version < 0 {
			bad(400, "InvalidParameter", fmt.Errorf("invalid stream id/version"))
			return
		}
		st := v.Streams[streamID]
		if action == "TableCreateWriteStream" {
			if st != nil && st.Version > version {
				bad(409, "InvalidState", fmt.Errorf("stale stream version"))
				return
			}
			if st == nil || st.Version < version {
				if len(v.Streams) >= 1024 {
					bad(429, "ResourceLimit", fmt.Errorf("stream limit"))
					return
				}
				if st != nil {
					v.Bytes -= st.Bytes
				}
				st = &storageStream{ID: streamID, Version: version, Exactly: req.ExactlyOnceMode, Requests: map[int64][32]byte{}}
				v.Streams[streamID] = st
			}
		}
		if st == nil || st.Version != version {
			bad(404, "NoSuchStream", fmt.Errorf("unknown stream/version"))
			return
		}
		if action == "TableCloseWriteStream" {
			st.Closed = true
			jsonResponse(w, 200, map[string]any{})
			return
		}
		if action == "TableWrite" {
			if st.Closed {
				bad(409, "InvalidState", fmt.Errorf("closed stream"))
				return
			}
			b, err := decodeBody(r)
			if err != nil {
				bad(400, "InvalidData", err)
				return
			}
			data, err := wire.DecodeArrow(b, storageWriteColumns(v), false)
			if err != nil {
				bad(400, "InvalidData", err)
				return
			}
			count, err := strconv.ParseInt(q.Get("Count"), 10, 64)
			if err != nil || count != int64(len(data.Rows)) {
				bad(400, "InvalidData", fmt.Errorf("Count mismatch"))
				return
			}
			hash := sha256.Sum256(b)
			offset := st.Offset
			if st.Exactly {
				offset, err = strconv.ParseInt(q.Get("RowOffset"), 10, 64)
				if err != nil || offset < 0 {
					bad(400, "InvalidParameter", fmt.Errorf("RowOffset required"))
					return
				}
				if old, ok := st.Requests[offset]; ok {
					if old != hash {
						bad(409, "RowOffsetConflict", fmt.Errorf("retry payload differs"))
						return
					}
					jsonResponse(w, 200, map[string]any{"ExactlyOnceRowOffset": st.Offset})
					return
				}
				if offset != st.Offset {
					bad(409, "RowOffsetConflict", fmt.Errorf("expected RowOffset %d", st.Offset))
					return
				}
				if len(st.Requests) >= 10000 {
					bad(429, "ResourceLimit", fmt.Errorf("retry history limit"))
					return
				}
			}
			if v.Bytes+data.Bytes > wire.MaxPayload {
				bad(429, "ResourceLimit", fmt.Errorf("staged data exceeds 64 MiB"))
				return
			}
			if strings.HasPrefix(mode, "Streaming") {
				if err := s.writeStorage(r.Context(), v, data.Rows, false); err != nil {
					bad(400, "InvalidData", err)
					return
				}
			} else {
				st.Rows = append(st.Rows, data.Rows...)
				v.Bytes += data.Bytes
				st.Bytes += data.Bytes
			}
			st.Offset += count
			if st.Exactly {
				st.Requests[offset] = hash
			}
			jsonResponse(w, 200, map[string]any{"ExactlyOnceRowOffset": st.Offset, "StagingId": strconv.FormatInt(st.Offset, 10)})
			return
		}
		jsonResponse(w, 200, map[string]any{"TableSchema": storageWriteSchema(v), "TableId": v.Meta.ID, "SchemaVersion": 1, "LatestSchemaVersion": 1, "RowOffset": st.Offset, "status": 0, "recordCount": st.Offset, "AccessToken": "emulator", "QuotaToken": "emulator"})
	default:
		bad(404, "UnsupportedOperation", fmt.Errorf("unsupported storage action %s", action))
	}
}
func projectResult(data engine.Result, names []string) (engine.Result, error) {
	if len(names) == 0 {
		return data, nil
	}
	out := engine.Result{Bytes: data.Bytes}
	indices := []int{}
	seen := map[string]bool{}
	for _, name := range names {
		found := -1
		for i, c := range data.Columns {
			if strings.EqualFold(c.Name, name) {
				found = i
				break
			}
		}
		if found < 0 || seen[strings.ToLower(name)] {
			return out, fmt.Errorf("unknown or duplicate column %s", name)
		}
		seen[strings.ToLower(name)] = true
		indices = append(indices, found)
		out.Columns = append(out.Columns, data.Columns[found])
	}
	for _, row := range data.Rows {
		r := []any{}
		for _, i := range indices {
			r = append(r, row[i])
		}
		out.Rows = append(out.Rows, r)
	}
	return out, nil
}

func storageWriteColumns(v *storageSession) []engine.Column {
	cols := append([]engine.Column{}, v.Meta.Columns...)
	for _, c := range v.Meta.Partitions {
		if _, ok := v.Part[c.Name]; !ok {
			cols = append(cols, c)
		}
	}
	if len(v.Meta.PrimaryKeys) > 0 {
		c, _ := engine.NewColumn("__operation", "tinyint")
		cols = append(cols, c)
	}
	return cols
}

func storageWriteSchema(v *storageSession) map[string]any {
	return writeSchema(storageWriteColumns(v))
}
func (s *Server) writeStorage(ctx context.Context, v *storageSession, rows [][]any, overwrite bool) error {
	var ops []byte
	if len(v.Meta.PrimaryKeys) > 0 {
		ops = make([]byte, len(rows))
		copyRows := make([][]any, len(rows))
		for i, row := range rows {
			if len(row) == 0 || row[len(row)-1] == nil {
				return fmt.Errorf("missing operation")
			}
			ops[i] = byte(wire.Int(row[len(row)-1]))
			copyRows[i] = row[:len(row)-1]
		}
		rows = copyRows
	}
	return s.Engine.WriteMutationsExpected(ctx, v.Project, v.Schema, v.Table, v.Meta.ID, v.Part, rows, overwrite, ops, nil)
}
