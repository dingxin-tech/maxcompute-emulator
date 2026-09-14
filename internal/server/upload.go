package server

import (
	"compress/zlib"
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
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type writeSession struct {
	Dynamic                                                    bool
	BlockCounts                                                map[int64]int
	RequestSequence                                            map[string]int64
	mu                                                         sync.Mutex
	ID, Project, Schema, Table, Partition, Kind, Status, Quota string
	Part                                                       map[string]string
	Meta                                                       engine.Table
	Blocks                                                     map[int64]engine.Result
	Created                                                    time.Time
	Bytes                                                      int64
	Sequence                                                   int64
	Requests                                                   map[string][32]byte
}

func decodeBody(r *http.Request) ([]byte, error) {
	var reader io.Reader = r.Body
	switch r.Header.Get("Content-Encoding") {
	case "", "identity":
	case "deflate":
		z, e := zlib.NewReader(r.Body)
		if e != nil {
			return nil, e
		}
		defer z.Close()
		reader = z
	case "zstd":
		z, e := zstd.NewReader(r.Body, zstd.WithDecoderMaxMemory(wire.MaxPayload), zstd.WithDecoderConcurrency(1))
		if e != nil {
			return nil, e
		}
		defer z.Close()
		reader = z
	case "lz4_frame", "x-lz4-frame", "x-odps-lz4-frame":
		reader = lz4.NewReader(r.Body)
	case "x-snappy-framed":
		reader = snappy.NewReader(r.Body)
	default:
		return nil, fmt.Errorf("unsupported Content-Encoding")
	}
	b, e := io.ReadAll(io.LimitReader(reader, wire.MaxPayload+1))
	if e != nil {
		return nil, e
	}
	if len(b) > wire.MaxPayload {
		return nil, fmt.Errorf("decoded payload exceeds 64 MiB")
	}
	return b, nil
}
func (s *Server) upload(w http.ResponseWriter, r *http.Request, p, sc, t, kind string) {
	q := r.URL.Query()
	sid := q.Get("uploadid")
	if kind == "upsert" {
		sid = q.Get("upsertid")
	}
	part, e := engine.ParsePartition(q.Get("partition"))
	if e != nil {
		fail(w, r, 400, "InvalidPartition", e)
		return
	}
	normalized, _ := json.Marshal(part)
	var v *writeSession
	if sid == "" {
		if r.Method != "POST" {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("missing upload session"))
			return
		}
		meta, e := s.Engine.Table(r.Context(), p, sc, t)
		if e != nil {
			fail(w, r, 404, "NoSuchTable", e)
			return
		}
		dynamic := kind == "stream" && q.Get("dynamic_partition") == "true"
		if !dynamic && len(part) != len(meta.Partitions) {
			fail(w, r, 400, "InvalidPartition", fmt.Errorf("complete partition required"))
			return
		}
		for _, c := range meta.Partitions {
			if _, ok := part[c.Name]; !ok && !dynamic {
				fail(w, r, 400, "InvalidPartition", fmt.Errorf("missing %s", c.Name))
				return
			}
		}
		if kind == "upsert" && len(meta.PrimaryKeys) == 0 {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("Upsert requires PRIMARY KEY"))
			return
		}
		v = &writeSession{ID: id(), Dynamic: dynamic, Project: p, Schema: sc, Table: strings.ToLower(t), Partition: string(normalized), Kind: kind, Status: "normal", Quota: q.Get("quotaName"), Part: part, Meta: meta, Blocks: map[int64]engine.Result{}, BlockCounts: map[int64]int{}, RequestSequence: map[string]int64{}, Created: time.Now(), Requests: map[string][32]byte{}}
		s.mu.Lock()
		for k, old := range s.writes {
			if time.Since(old.Created) > s.cfg.SessionTTL {
				delete(s.writes, k)
			}
		}
		if len(s.writes) >= s.cfg.MaxSessions {
			s.mu.Unlock()
			fail(w, r, 429, "ResourceLimit", fmt.Errorf("write session limit"))
			return
		}
		s.writes[v.ID] = v
		s.mu.Unlock()
		jsonResponse(w, 200, writeJSON(v, r.Host))
		return
	}
	s.mu.Lock()
	v = s.writes[sid]
	s.mu.Unlock()
	if v == nil || v.Kind != kind || v.Project != p || v.Schema != sc || v.Table != strings.ToLower(t) || (!v.Dynamic && v.Partition != string(normalized)) || time.Since(v.Created) > s.cfg.SessionTTL {
		fail(w, r, 404, "NoSuchUpload", fmt.Errorf("unknown or expired upload"))
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	switch r.Method {
	case "GET":
		jsonResponse(w, 200, writeJSON(v, r.Host))
	case "DELETE":
		if v.Status == "committed" || v.Status == "closed" {
			fail(w, r, 409, "InvalidState", fmt.Errorf("already committed"))
			return
		}
		v.Status = "aborted"
		v.Blocks = map[int64]engine.Result{}
		v.Bytes = 0
		jsonResponse(w, 200, writeJSON(v, r.Host))
	case "POST":
		if v.Status == "aborted" {
			fail(w, r, 409, "InvalidState", fmt.Errorf("aborted session"))
			return
		}
		if v.Status == "normal" {
			keys := blockIDs(v)
			rows := [][]any{}
			for _, k := range keys {
				rows = append(rows, v.Blocks[k].Rows...)
			}
			var ops []byte
			var partial [][]int
			if kind == "upsert" {
				ops = make([]byte, len(rows))
				partial = make([][]int, len(rows))
				n := len(v.Meta.Columns)
				for i, row := range rows {
					if row[n+2] == nil {
						fail(w, r, 400, "InvalidData", fmt.Errorf("missing operation"))
						return
					}
					ops[i] = byte(wire.Int(row[n+2]))
					if cols, ok := row[n+4].([]any); ok {
						for _, col := range cols {
							partial[i] = append(partial[i], int(wire.Int(col)))
						}
					}
					rows[i] = row[:n]
				}
			}
			if e = s.Engine.WriteMutations(r.Context(), p, sc, t, v.Part, rows, false, ops, partial); e != nil {
				fail(w, r, 400, "InvalidData", e)
				return
			}
			v.Status = "closed"
			if kind == "upsert" {
				v.Status = "committed"
			}
			v.Bytes = 0
			for k, b := range v.Blocks {
				b.Rows = nil
				b.Bytes = 0
				v.Blocks[k] = b
			}
		}
		jsonResponse(w, 200, writeJSON(v, r.Host))
	case "PUT":
		if v.Status != "normal" {
			fail(w, r, 409, "InvalidState", fmt.Errorf("session is %s", v.Status))
			return
		}
		body, e := decodeBody(r)
		if e != nil {
			fail(w, r, 400, "InvalidData", e)
			return
		}
		trace := r.Header.Get("odps-tunnel-retry-trace-id")
		if trace != "" {
			trace += string(normalized)
		}
		hash := sha256.Sum256(body)
		if trace != "" {
			if old, ok := v.Requests[trace]; ok {
				if old != hash {
					fail(w, r, 409, "InvalidData", fmt.Errorf("retry payload differs"))
					return
				}
				writeHeaders(w, r.Host, v.RequestSequence[trace])
				jsonResponse(w, 200, map[string]any{"ContentHash": fmt.Sprintf("%x", hash)})
				return
			}
			if len(v.Requests) >= 10000 {
				fail(w, r, 429, "ResourceLimit", fmt.Errorf("retry history limit"))
				return
			}
		}
		cols := append([]engine.Column{}, v.Meta.Columns...)
		if kind == "upsert" {
			for _, spec := range [][2]string{{"__version", "bigint"}, {"__app_version", "bigint"}, {"__operation", "tinyint"}, {"__key_cols", "array<bigint>"}, {"__value_cols", "array<bigint>"}} {
				c, _ := engine.NewColumn(spec[0], spec[1])
				cols = append(cols, c)
			}
		}
		var data engine.Result
		if q.Has("arrow") {
			data, e = wire.DecodeArrow(body, cols, true)
		} else {
			data, e = wire.DecodeProtobuf(body, cols)
		}
		if e != nil {
			fail(w, r, 400, "InvalidData", e)
			return
		}
		if q.Has("record_count") {
			n, e := strconv.Atoi(q.Get("record_count"))
			if e != nil || n != len(data.Rows) {
				fail(w, r, 400, "InvalidData", fmt.Errorf("record_count mismatch"))
				return
			}
		}
		block := v.Sequence
		if kind == "batch" {
			if len(v.Blocks) >= 1024 {
				fail(w, r, 429, "ResourceLimit", fmt.Errorf("block limit"))
				return
			}
			block, e = strconv.ParseInt(q.Get("blockid"), 10, 64)
			if e != nil || block < 0 {
				fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid blockid"))
				return
			}
		}
		if v.Bytes-v.Blocks[block].Bytes+data.Bytes > wire.MaxPayload {
			fail(w, r, 429, "ResourceLimit", fmt.Errorf("write session exceeds 64 MiB"))
			return
		}
		if kind == "stream" {
			if e = s.Engine.Write(r.Context(), p, sc, t, part, data.Rows, false, nil); e != nil {
				fail(w, r, 400, "InvalidData", e)
				return
			}
		} else {
			v.Bytes += data.Bytes - v.Blocks[block].Bytes
			v.Blocks[block] = data
			v.BlockCounts[block] = len(data.Rows)
		}
		v.Sequence++
		if trace != "" {
			v.Requests[trace] = hash
			v.RequestSequence[trace] = v.Sequence
		}
		writeHeaders(w, r.Host, v.Sequence)
		jsonResponse(w, 200, map[string]any{"RecordCount": len(data.Rows), "ContentHash": fmt.Sprintf("%x", sha256.Sum256(body))})
	default:
		fail(w, r, 405, "InvalidMethod", fmt.Errorf("unsupported upload method"))
	}
}
func blockIDs(v *writeSession) []int64 {
	ks := []int64{}
	for k := range v.Blocks {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
	return ks
}
func writeJSON(v *writeSession, host string) map[string]any {
	columns := []any{}
	for i, c := range v.Meta.Columns {
		columns = append(columns, map[string]any{"name": c.Name, "type": c.Type, "nullable": c.Nullable, "comment": c.Comment, "column_id": i})
	}
	schema := map[string]any{"columns": columns, "partitionKeys": v.Meta.Partitions}
	if v.Kind == "stream" {
		return map[string]any{"session_name": v.ID, "schema": schema, "schema_version": "1", "slots": [][]string{{"0", host}}, "status": v.Status, "quota_name": v.Quota}
	}
	if v.Kind == "upsert" {
		return map[string]any{"id": v.ID, "schema": schema, "hash_key": v.Meta.PrimaryKeys, "hasher": "default", "slots": []any{map[string]any{"slot_id": "0", "worker_addr": host, "buckets": []int{0}}}, "status": v.Status, "quota_name": v.Quota, "enable_partial_update": true}
	}
	blocks := []any{}
	for _, k := range blockIDs(v) {
		blocks = append(blocks, map[string]any{"BlockID": k, "RecordCount": v.BlockCounts[k]})
	}
	return map[string]any{"UploadID": v.ID, "Status": v.Status, "UploadedBlockList": blocks, "Schema": schema, "MaxFieldSize": 8 << 20, "QuotaName": v.Quota}
}

// Java SDK preserves case for these non-x headers and looks them up literally.
func writeHeaders(w http.ResponseWriter, host string, sequence int64) {
	w.Header()["odps-tunnel-slot-num"] = []string{"1"}
	w.Header()["odps-tunnel-routed-server"] = []string{host}
	w.Header()["odps-tunnel-batch-id"] = []string{strconv.FormatInt(sequence, 10)}
	w.Header().Set("x-odps-tunnel-schema-version", "1")
}
