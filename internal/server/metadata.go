package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

func (s *Server) partitionMetadata(w http.ResponseWriter, r *http.Request, p, sc string, t engine.Table) {
	all, err := s.Engine.Partitions(r.Context(), p, sc, t.Name)
	if err != nil {
		fail(w, r, 400, "InvalidParameter", err)
		return
	}
	q := r.URL.Query()
	filter := map[string]string{}
	if q.Get("partition") != "" {
		filter, err = engine.ParsePartition(q.Get("partition"))
		if err != nil {
			fail(w, r, 400, "InvalidParameter", err)
			return
		}
	}
	known := map[string]bool{}
	for _, c := range t.Partitions {
		known[c.Name] = true
	}
	for k := range filter {
		if !known[k] {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("unknown partition key"))
			return
		}
	}
	if !q.Has("partitions") && len(filter) != len(t.Partitions) {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("complete partition required"))
		return
	}
	type entry struct{ key, xml string }
	entries := []entry{}
	for _, part := range all {
		match := true
		for k, v := range filter {
			if part[k] != v {
				match = false
			}
		}
		if !match {
			continue
		}
		var cols, keys strings.Builder
		for i, c := range t.Partitions {
			if i > 0 {
				keys.WriteString(",")
			}
			keys.WriteString(c.Name + "='" + strings.ReplaceAll(part[c.Name], "'", "''") + "'")
			fmt.Fprintf(&cols, "<Column Name=\"%s\" Value=\"%s\"/>", esc(c.Name), esc(part[c.Name]))
		}
		entries = append(entries, entry{keys.String(), "<Partition>" + cols.String() + "</Partition>"})
	}
	if !q.Has("partitions") {
		if len(entries) == 0 {
			fail(w, r, 404, "NoSuchPartition", fmt.Errorf("partition does not exist"))
			return
		}
		meta, _ := json.Marshal(map[string]any{"createTime": t.Created, "lastDDLTime": t.Created, "lastModifiedTime": t.Created})
		xmlResponse(w, "<Partition><Schema>"+esc(string(meta))+"</Schema></Partition>")
		return
	}
	limit := 1000
	if q.Get("maxitems") != "" {
		limit, err = strconv.Atoi(q.Get("maxitems"))
		if err != nil || limit < 1 || limit > 10000 {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("maxitems must be 1..10000"))
			return
		}
	}
	var body strings.Builder
	marker := ""
	count := 0
	for _, entry := range entries {
		if entry.key <= q.Get("marker") {
			continue
		}
		if count == limit {
			break
		}
		body.WriteString(entry.xml)
		marker = entry.key
		count++
	}
	hasMore := false
	for _, entry := range entries {
		if entry.key > marker && entry.key > q.Get("marker") {
			hasMore = true
		}
	}
	if !hasMore {
		marker = ""
	}
	xmlResponse(w, fmt.Sprintf("<Partitions><Marker>%s</Marker><MaxItems>%d</MaxItems>%s</Partitions>", esc(marker), limit, body.String()))
}

func (s *Server) listTables(w http.ResponseWriter, r *http.Request, p, sc string, tables []engine.Table) {
	q := r.URL.Query()
	limit := 1000
	var err error
	if q.Get("maxitems") != "" {
		limit, err = strconv.Atoi(q.Get("maxitems"))
		if err != nil || limit < 1 || limit > 10000 {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("maxitems must be 1..10000"))
			return
		}
	}
	var b strings.Builder
	marker := ""
	count := 0
	more := false
	for _, t := range tables {
		if !strings.HasPrefix(t.Name, q.Get("name")) || t.Name <= q.Get("marker") {
			continue
		}
		if count == limit {
			more = true
			break
		}
		b.WriteString(tableXML(p, sc, t))
		marker = t.Name
		count++
	}
	if !more {
		marker = ""
	}
	xmlResponse(w, fmt.Sprintf("<Tables><Marker>%s</Marker><MaxItems>%d</MaxItems>%s</Tables>", esc(marker), limit, b.String()))
}
