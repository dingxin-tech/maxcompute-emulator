package server

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
)

// BatchCompatible retains only the newest attempt of each numbered block.
// Commit messages are opaque tokens bound to this session and attempt.
func (s *Server) storageBlocks(w http.ResponseWriter, r *http.Request, v *storageSession, req storageRequest) {
	failWith := func(code int, err error) { fail(w, r, code, "InvalidState", err) }
	response := func() {
		jsonResponse(w, 200, map[string]any{"SessionId": v.ID, "SessionStatus": v.Status, "DataSchema": readSchema(storageWriteColumns(v)), "MaxBlockNumber": 1024, "EnhanceWriteCheck": true})
	}
	switch r.URL.Query().Get("Action") {
	case "TableCreateWriteSession", "TableGetWriteSession":
		response()
	case "TableCreateWriteStream":
		if v.Status != "NORMAL" {
			failWith(409, fmt.Errorf("session not open"))
			return
		}
		w.Header().Set("x-odps-max-storage-route-token", v.ID)
		jsonResponse(w, 200, map[string]any{"TableSchema": writeSchema(storageWriteColumns(v)), "QuotaToken": v.ID})
	case "TableWrite":
		if v.Status != "NORMAL" {
			failWith(409, fmt.Errorf("session not open"))
			return
		}
		q := r.URL.Query()
		block, e := strconv.Atoi(q.Get("BlockNumber"))
		attempt, ae := strconv.ParseInt(q.Get("AttemptNumber"), 10, 64)
		if e != nil || ae != nil || block < 0 || block >= 1024 || attempt < 0 {
			failWith(400, fmt.Errorf("invalid block/attempt"))
			return
		}
		key := strconv.Itoa(block)
		old := v.Streams[key]
		if old != nil && old.Version > attempt {
			failWith(409, fmt.Errorf("stale attempt"))
			return
		}
		b, e := decodeBody(r)
		if e != nil {
			failWith(400, e)
			return
		}
		hash := sha256.Sum256(b)
		if old != nil && old.Version == attempt {
			if old.Hash != hash {
				failWith(409, fmt.Errorf("attempt already written with different data"))
				return
			}
			jsonResponse(w, 200, map[string]any{"CommitMessage": old.Token, "RecordCount": old.Offset})
			return
		}
		data, e := wire.DecodeArrow(b, storageWriteColumns(v), false)
		if e != nil {
			failWith(400, e)
			return
		}
		previous := int64(0)
		if old != nil {
			previous = old.Bytes
		}
		if v.Bytes-previous+data.Bytes > wire.MaxPayload {
			failWith(429, fmt.Errorf("staged data exceeds 64 MiB"))
			return
		}
		token, _ := json.Marshal(map[string]any{"SessionId": v.ID, "BlockNumber": block, "AttemptNumber": attempt, "Nonce": id(), "WriterStats": map[string]any{"RecordNum": len(data.Rows)}})
		st := &storageStream{ID: key, Version: attempt, Token: string(token), Hash: hash, Rows: data.Rows, Bytes: data.Bytes, Offset: int64(len(data.Rows)), Closed: true}
		v.Streams[key] = st
		v.Bytes += data.Bytes - previous
		jsonResponse(w, 200, map[string]any{"CommitMessage": st.Token, "RecordCount": st.Offset})
	case "TableCommitWriteSession":
		if v.Status == "COMMITTED" {
			response()
			return
		}
		if v.Status != "NORMAL" {
			failWith(409, fmt.Errorf("session not open"))
			return
		}
		rows := [][]any{}
		seen := map[string]bool{}
		for _, token := range req.CommitMessages {
			var match *storageStream
			for _, st := range v.Streams {
				if st.Token == token {
					match = st
					break
				}
			}
			if match == nil || seen[token] {
				failWith(400, fmt.Errorf("unknown/duplicate commit message"))
				return
			}
			seen[token] = true
			rows = append(rows, match.Rows...)
		}
		if err := s.writeStorage(r.Context(), v, rows, v.Overwrite); err != nil {
			failWith(400, err)
			return
		}
		v.Status = "COMMITTED"
		v.Bytes = 0
		for _, st := range v.Streams {
			st.Rows = nil
			st.Bytes = 0
		}
		response()
	case "TableAbortWriteSession":
		if v.Status == "COMMITTED" {
			failWith(409, fmt.Errorf("already committed"))
			return
		}
		v.Status = "ABORTED"
		v.Streams = map[string]*storageStream{}
		v.Bytes = 0
		response()
	default:
		failWith(404, fmt.Errorf("unsupported batch-compatible action"))
	}
}
