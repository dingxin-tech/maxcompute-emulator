package server

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/dingxin-tech/maxcompute-emulator/internal/wire"
)

func (s *Server) instanceDownload(w http.ResponseWriter, r *http.Request, p, sc, iid string) {
	if r.URL.Query().Has("downloadid") {
		s.download(w, r, p, sc, "@instance_"+iid)
		return
	}
	if r.Method != "POST" {
		fail(w, r, 405, "InvalidMethod", fmt.Errorf("expected POST"))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.instances[iid]
	if i == nil || i.Project != p || i.Schema != sc || time.Since(i.Created) > s.cfg.SessionTTL {
		fail(w, r, 404, "NoSuchInstance", fmt.Errorf("unknown instance"))
		return
	}
	if i.Status != "Success" {
		fail(w, r, 409, "InvalidState", fmt.Errorf("instance failed"))
		return
	}
	if !s.downloadCapacityLocked(i.Data.Bytes) {
		fail(w, r, 429, "ResourceLimit", fmt.Errorf("session limit"))
		return
	}
	v := &session{ID: id(), Project: p, Schema: sc, Table: "@instance_" + iid, Partition: "{}", Data: i.Data, Meta: engine.Table{Columns: i.Data.Columns, Partitions: []engine.Column{}}, Created: time.Now()}
	s.sessions[v.ID] = v
	jsonResponse(w, 200, sessionJSON(v))
}
func (s *Server) storageInstance(w http.ResponseWriter, r *http.Request, p, iid string) {
	q := r.URL.Query()
	action := q.Get("Action")
	s.mu.Lock()
	i := s.instances[iid]
	v := s.sessions[q.Get("SessionId")]
	s.mu.Unlock()
	if i == nil || i.Project != p || time.Since(i.Created) > s.cfg.SessionTTL {
		fail(w, r, 404, "NoSuchInstance", fmt.Errorf("unknown instance"))
		return
	}
	if i.Status != "Success" {
		fail(w, r, 409, "InvalidState", fmt.Errorf("instance failed"))
		return
	}
	if action == "InstanceCreateReadSession" {
		v = &session{ID: id(), Project: p, Schema: i.Schema, Table: "@instance_" + iid, Partition: "{}", Data: i.Data, Meta: engine.Table{Columns: i.Data.Columns, Partitions: []engine.Column{}}, Created: time.Now()}
		s.mu.Lock()
		if !s.downloadCapacityLocked(i.Data.Bytes) {
			s.mu.Unlock()
			fail(w, r, 429, "ResourceLimit", fmt.Errorf("session limit"))
			return
		}
		s.sessions[v.ID] = v
		s.mu.Unlock()
	}
	if v == nil || v.Project != p || v.Table != "@instance_"+iid || time.Since(v.Created) > s.cfg.SessionTTL {
		fail(w, r, 404, "NoSuchSession", fmt.Errorf("unknown instance session"))
		return
	}
	switch action {
	case "InstanceCreateReadSession", "InstanceGetReadSession":
		jsonResponse(w, 200, map[string]any{"DownloadID": v.ID, "RecordCount": len(v.Data.Rows), "Status": "normal", "TableSchema": writeSchema(v.Data.Columns), "QuotaName": ""})
	case "InstanceRead":
		start, n := int64(0), int64(len(v.Data.Rows))
		var err error
		if q.Has("Offset") {
			start, err = strconv.ParseInt(q.Get("Offset"), 10, 64)
		}
		if err == nil && q.Has("Count") {
			n, err = strconv.ParseInt(q.Get("Count"), 10, 64)
		}
		if err != nil || start < 0 || start > int64(len(v.Data.Rows)) || n < 0 {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid row range"))
			return
		}
		n = min(n, int64(len(v.Data.Rows))-start)
		data := v.Data
		data.Rows = data.Rows[start : start+n]
		b, err := wire.Arrow(data, 4096, false)
		if err != nil {
			fail(w, r, 400, "InvalidData", err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(b)
	default:
		fail(w, r, 404, "UnsupportedOperation", fmt.Errorf("unknown instance action"))
	}
}

// Caller holds s.mu. Instance downloads share the same capacity as table downloads.
func (s *Server) downloadCapacityLocked(bytes int64) bool {
	var retained int64
	for key, old := range s.sessions {
		if time.Since(old.Created) > s.cfg.SessionTTL {
			delete(s.sessions, key)
		} else {
			retained += old.Data.Bytes
		}
	}
	return len(s.sessions) < s.cfg.MaxSessions && retained+bytes <= 256<<20
}
