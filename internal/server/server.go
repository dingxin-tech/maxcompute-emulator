package server

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
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

const Version = "2.1.0"

type Config struct {
	SessionTTL     time.Duration
	MaxSessions    int
	PublicEndpoint string
	QueryTimeout   time.Duration
}
type session struct {
	ID, Project, Schema, Table, Partition, Quota string
	Data                                         engine.Result
	Meta                                         engine.Table
	Created                                      time.Time
}
type instance struct {
	Data                                             engine.Result
	Project, Schema, ID, Name, Query, Status, Output string
	Created                                          time.Time
}
type Server struct {
	Engine    *engine.Engine
	cfg       Config
	mu        sync.Mutex
	sessions  map[string]*session
	instances map[string]*instance
	writes    map[string]*writeSession
	storage   map[string]*storageSession
	inflight  chan struct{}
}

func New(e *engine.Engine, c Config) *Server {
	if c.SessionTTL == 0 {
		c.SessionTTL = 30 * time.Minute
	}
	if c.MaxSessions == 0 {
		c.MaxSessions = 64
	}
	if c.QueryTimeout == 0 {
		c.QueryTimeout = 30 * time.Second
	}
	return &Server{Engine: e, cfg: c, sessions: map[string]*session{}, instances: map[string]*instance{}, writes: map[string]*writeSession{}, storage: map[string]*storageSession{}, inflight: make(chan struct{}, 16)}
}
func id() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func esc(s string) string { var b bytes.Buffer; xml.EscapeText(&b, []byte(s)); return b.String() }
func jsonResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func xmlResponse(w http.ResponseWriter, s string) {
	w.Header().Set("Content-Type", "application/xml")
	io.WriteString(w, s)
}
func fail(w http.ResponseWriter, r *http.Request, status int, code string, e error) {
	message := e.Error()
	if len(message) > 600 {
		message = message[:600]
	}
	if r.URL.Query().Has("downloads") || r.URL.Query().Has("downloadid") || r.URL.Query().Has("uploads") || r.URL.Query().Has("uploadid") || strings.Contains(r.URL.Path, "/upserts") || strings.Contains(r.URL.Path, "/streams") || strings.Contains(r.URL.Path, "storage") {
		jsonResponse(w, status, map[string]string{"Code": code, "Message": message, "RequestId": w.Header().Get("x-odps-request-id")})
	} else {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		fmt.Fprintf(w, "<Error><Code>%s</Code><Message>%s</Message><RequestId>%s</RequestId></Error>", esc(code), esc(message), w.Header().Get("x-odps-request-id"))
	}
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	select {
	case s.inflight <- struct{}{}:
		defer func() { <-s.inflight }()
	default:
		w.Header().Set("Retry-After", "1")
		http.Error(w, "request capacity exceeded", http.StatusServiceUnavailable)
		return
	}
	start := time.Now()
	requestID := id()
	w.Header().Set("x-odps-request-id", requestID)
	w.Header().Set("x-odps-tunnel-version", "5")
	w.Header().Set("Last-Modified", start.UTC().Format(http.TimeFormat))
	defer func() {
		if e := recover(); e != nil {
			slog.Error("request panic", "request_id", requestID, "error", e)
			fail(w, r, 500, "InternalError", fmt.Errorf("request failed"))
		}
		slog.Info("request", "method", r.Method, "path", r.URL.Path, "request_id", requestID, "elapsed_ms", time.Since(start).Milliseconds())
	}()
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	r.Body = http.MaxBytesReader(w, r.Body, wire.MaxPayload)
	path := strings.Trim(r.URL.Path, "/")
	if strings.HasPrefix(path, "api/") {
		path = strings.TrimPrefix(path, "api/")
	}
	if path == "healthz" || path == "readyz" || path == "init" {
		if path == "init" && r.Method == "POST" {
			b, e := io.ReadAll(r.Body)
			if e != nil {
				fail(w, r, 400, "InvalidParameter", e)
				return
			}
			u, e := url.Parse(strings.Trim(string(b), "\" \n"))
			if e != nil || u.Host == "" {
				fail(w, r, 400, "InvalidParameter", fmt.Errorf("expected http endpoint"))
				return
			}
			s.mu.Lock()
			s.cfg.PublicEndpoint = u.String()
			s.mu.Unlock()
		}
		jsonResponse(w, 200, map[string]string{"status": "ready", "version": Version})
		return
	}
	if path == "storage/v2" || path == "storage/v3" {
		s.storageAPI(w, r)
		return
	}
	if path == "capabilities" {
		jsonResponse(w, 200, map[string]any{"version": Version, "tunnel_download": []string{"create", "reload", "protobuf", "arrow", "complete"}, "compression": []string{"identity", "deflate", "zstd", "lz4_frame"}, "storage_v2": true, "storage_paths": []string{"/api/storage/v2", "/api/storage/v3"}, "tunnel_upload": []string{"protobuf", "arrow", "blocks", "stream", "upsert"}, "storage_write_modes": []string{"Batch", "BatchCompatible", "Streaming", "StreamingRealtime"}, "sql": "CREATE/DROP/INSERT/SELECT; static partitions; ODPS2 subset", "auth": "test-only; signatures not validated"})
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[0] != "projects" {
		fail(w, r, 404, "UnsupportedOperation", fmt.Errorf("unsupported endpoint"))
		return
	}
	project := parts[1]
	schema := r.URL.Query().Get("curr_schema")
	if schema == "" {
		schema = "default"
	}
	rest := parts[2:]
	if len(rest) >= 2 && rest[0] == "schemas" {
		schema = rest[1]
		rest = rest[2:]
	}
	if len(rest) == 0 && r.Method == "GET" {
		w.Header().Set("x-odps-owner", "emulator")
		w.Header().Set("x-odps-creation-time", start.UTC().Format(http.TimeFormat))
		xmlResponse(w, "<Project><Name>"+esc(project)+"</Name><Owner>emulator</Owner><Properties><Property><Name>odps.schema.model.enabled</Name><Value>true</Value></Property></Properties></Project>")
		return
	}
	if len(rest) == 1 && rest[0] == "tunnel" && r.Method == "GET" {
		s.mu.Lock()
		ep := s.cfg.PublicEndpoint
		s.mu.Unlock()
		host := r.Host
		if ep != "" {
			u, _ := url.Parse(ep)
			host = u.Host
		}
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, host)
		return
	}
	if len(rest) >= 1 && rest[0] == "instances" {
		s.instance(w, r, project, schema, rest[1:])
		return
	}
	if len(rest) >= 1 && rest[0] == "tables" {
		if len(rest) == 3 && (rest[2] == "streams" || rest[2] == "upserts") {
			kind := "stream"
			if rest[2] == "upserts" {
				kind = "upsert"
			}
			s.upload(w, r, project, schema, rest[1], kind)
			return
		}
		if len(rest) == 2 {
			s.table(w, r, project, schema, rest[1])
			return
		}
		if len(rest) == 1 && r.Method == "GET" {
			tables, e := s.Engine.Tables(ctx, project, schema)
			if e != nil {
				fail(w, r, 500, "InternalError", e)
				return
			}
			var b strings.Builder
			b.WriteString("<Tables><Marker></Marker><MaxItems>10000</MaxItems>")
			for _, t := range tables {
				b.WriteString(tableXML(project, schema, t))
			}
			b.WriteString("</Tables>")
			xmlResponse(w, b.String())
			return
		}
	}
	fail(w, r, 404, "UnsupportedOperation", fmt.Errorf("unsupported endpoint"))
}
func tableXML(p, s string, t engine.Table) string {
	b, _ := json.Marshal(map[string]any{"columns": t.Columns, "partitionKeys": t.Partitions})
	return "<Table><Name>" + esc(t.Name) + "</Name><TableId>" + esc(t.Name) + "</TableId><Project>" + esc(p) + "</Project><SchemaName>" + esc(s) + "</SchemaName><Owner>emulator</Owner><Type>MANAGED_TABLE</Type><Schema>" + esc(string(b)) + "</Schema><Comment></Comment></Table>"
}
func (s *Server) table(w http.ResponseWriter, r *http.Request, p, sc, t string) {
	q := r.URL.Query()
	if q.Has("uploads") || q.Has("uploadid") {
		s.upload(w, r, p, sc, t, "batch")
		return
	}
	if q.Has("downloads") && r.Method == "POST" {
		s.createDownload(w, r, p, sc, t)
		return
	}
	if q.Has("downloadid") {
		s.download(w, r, p, sc, t)
		return
	}
	if r.Method != "GET" {
		fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("unsupported table operation"))
		return
	}
	table, e := s.Engine.Table(r.Context(), p, sc, t)
	if e != nil {
		fail(w, r, 404, "NoSuchTable", e)
		return
	}
	if q.Has("partitions") || q.Has("partition") {
		fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("partition listing not in CK Tunnel M1; specify partition when downloading"))
		return
	}
	xmlResponse(w, tableXML(p, sc, table))
}
func (s *Server) instance(w http.ResponseWriter, r *http.Request, p, sc string, rest []string) {
	if len(rest) == 0 && r.Method == "POST" {
		var req struct {
			Job struct {
				Tasks struct {
					SQL []struct {
						Name  string `xml:"Name"`
						Query string `xml:"Query"`
					} `xml:"SQL"`
				} `xml:"Tasks"`
			} `xml:"Job"`
		}
		if e := xml.NewDecoder(r.Body).Decode(&req); e != nil || len(req.Job.Tasks.SQL) != 1 {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("one SQL task in Instance/Job/Tasks required"))
			return
		}
		task := req.Job.Tasks.SQL[0]
		if task.Name == "" {
			task.Name = "AnonymousSQLTask"
		}
		i := &instance{Project: p, Schema: sc, ID: id(), Name: task.Name, Query: task.Query, Status: "Success", Created: time.Now()}
		res, e := s.Engine.Execute(r.Context(), p, sc, task.Query)
		i.Data = res
		if e != nil {
			i.Status = "Failed"
			i.Output = e.Error()
		} else {
			var b bytes.Buffer
			cw := csv.NewWriter(&b)
			for _, row := range res.Rows {
				line := make([]string, len(row))
				for n, v := range row {
					if v != nil {
						line[n] = fmt.Sprint(v)
					} else {
						line[n] = "\\N"
					}
				}
				cw.Write(line)
			}
			cw.Flush()
			i.Output = b.String()
		}
		s.mu.Lock()
		for k, old := range s.instances {
			if time.Since(old.Created) > s.cfg.SessionTTL {
				delete(s.instances, k)
			}
		}
		if len(s.instances) >= 10000 {
			s.mu.Unlock()
			fail(w, r, 429, "ResourceLimit", fmt.Errorf("instance limit"))
			return
		}
		s.instances[i.ID] = i
		s.mu.Unlock()
		w.Header().Set("Location", "/projects/"+url.PathEscape(p)+"/instances/"+i.ID)
		w.WriteHeader(201)
		return
	}
	if len(rest) == 1 && (r.URL.Query().Has("downloads") || r.URL.Query().Has("downloadid")) {
		s.instanceDownload(w, r, p, sc, rest[0])
		return
	}
	if len(rest) != 1 || r.Method != "GET" {
		fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("unsupported instance operation"))
		return
	}
	s.mu.Lock()
	i := s.instances[rest[0]]
	s.mu.Unlock()
	if i == nil || i.Project != p || i.Schema != sc {
		fail(w, r, 404, "NoSuchInstance", fmt.Errorf("unknown instance"))
		return
	}
	now := i.Created.UTC().Format(http.TimeFormat)
	w.Header().Set("x-odps-start-time", now)
	w.Header().Set("x-odps-end-time", now)
	w.Header().Set("x-odps-owner", "emulator")
	if r.URL.Query().Has("source") {
		xmlResponse(w, "<Job><Tasks><SQL><Name>"+esc(i.Name)+"</Name><Query>"+esc(i.Query)+"</Query></SQL></Tasks></Job>")
		return
	}
	xmlResponse(w, "<Instance><Status>Terminated</Status><Tasks><Task Type=\"SQL\"><Name>"+esc(i.Name)+"</Name><Status>"+i.Status+"</Status><StartTime>"+now+"</StartTime><EndTime>"+now+"</EndTime><Result>"+esc(i.Output)+"</Result></Task></Tasks></Instance>")
}
func (s *Server) createDownload(w http.ResponseWriter, r *http.Request, p, sc, t string) {
	part, err := engine.ParsePartition(r.URL.Query().Get("partition"))
	if err != nil {
		fail(w, r, 400, "InvalidPartition", err)
		return
	}
	data, meta, err := s.Engine.Snapshot(r.Context(), p, sc, t, part)
	if err != nil {
		code, status := "InvalidParameter", 400
		if strings.Contains(err.Error(), "NoSuchTable") {
			code, status = "NoSuchTable", 404
		}
		fail(w, r, status, code, err)
		return
	}
	partJSON, _ := json.Marshal(part)
	sess := &session{ID: id(), Project: p, Schema: sc, Table: strings.ToLower(t), Partition: string(partJSON), Quota: r.URL.Query().Get("quotaName"), Data: data, Meta: meta, Created: time.Now()}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.sessions {
		if time.Since(v.Created) > s.cfg.SessionTTL {
			delete(s.sessions, k)
		}
	}
	var totalBytes int64
	for _, v := range s.sessions {
		totalBytes += v.Data.Bytes
	}
	if len(s.sessions) >= s.cfg.MaxSessions || totalBytes+data.Bytes > 256<<20 {
		fail(w, r, 429, "ResourceLimit", fmt.Errorf("session limit reached; close sessions"))
		return
	}
	w.Header().Set("Last-Modified", sess.Created.UTC().Format(http.TimeFormat))
	s.sessions[sess.ID] = sess
	jsonResponse(w, 200, sessionJSON(sess))
}
func sessionJSON(s *session) map[string]any {
	return map[string]any{"DownloadID": s.ID, "RecordCount": len(s.Data.Rows), "Status": "normal", "Owner": "emulator", "Initiated": s.Created.UTC().Format(time.RFC3339), "Schema": map[string]any{"columns": s.Meta.Columns, "partitionKeys": s.Meta.Partitions, "IsVirtualView": false}, "QuotaName": s.Quota}
}
func (s *Server) download(w http.ResponseWriter, r *http.Request, p, sc, t string) {
	q := r.URL.Query()
	part, e := engine.ParsePartition(q.Get("partition"))
	if e != nil {
		fail(w, r, 400, "InvalidPartition", e)
		return
	}
	partJSON, _ := json.Marshal(part)
	s.mu.Lock()
	v := s.sessions[q.Get("downloadid")]
	if v == nil || time.Since(v.Created) > s.cfg.SessionTTL || v.Project != p || v.Schema != sc || v.Table != strings.ToLower(t) || v.Partition != string(partJSON) {
		s.mu.Unlock()
		fail(w, r, 404, "NoSuchDownload", fmt.Errorf("session missing, expired, closed, or bound to another table/partition"))
		return
	}
	w.Header().Set("Last-Modified", v.Created.UTC().Format(http.TimeFormat))
	if r.Method == "POST" {
		delete(s.sessions, v.ID)
		s.mu.Unlock()
		w.WriteHeader(200)
		return
	}
	s.mu.Unlock()
	if r.Method != "GET" {
		fail(w, r, 405, "InvalidMethod", fmt.Errorf("GET or POST required"))
		return
	}
	if !q.Has("data") {
		jsonResponse(w, 200, sessionJSON(v))
		return
	}
	rangeText := q.Get("rowrange")
	if len(rangeText) < 5 || rangeText[0] != '(' || rangeText[len(rangeText)-1] != ')' {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("rowrange=(start,count) required"))
		return
	}
	a := strings.Split(rangeText[1:len(rangeText)-1], ",")
	if len(a) != 2 {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid rowrange"))
		return
	}
	start, e1 := strconv.ParseUint(a[0], 10, 63)
	count, e2 := strconv.ParseUint(a[1], 10, 63)
	if e1 != nil || e2 != nil || start > uint64(len(v.Data.Rows)) {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid range"))
		return
	}
	end := uint64(len(v.Data.Rows))
	if count < end-start {
		end = start + count
	}
	res := engine.Result{Columns: v.Data.Columns, Rows: v.Data.Rows[start:end]}
	if names := q.Get("columns"); names != "" {
		indices := []int{}
		cols := []engine.Column{}
		seen := map[string]bool{}
		for _, n := range strings.Split(names, ",") {
			n = strings.ToLower(strings.TrimSpace(n))
			idx := -1
			for j, c := range res.Columns {
				if c.Name == n {
					idx = j
				}
			}
			if idx < 0 || seen[n] {
				fail(w, r, 400, "InvalidColumn", fmt.Errorf("unknown or duplicate column %s", n))
				return
			}
			seen[n] = true
			indices = append(indices, idx)
			cols = append(cols, res.Columns[idx])
		}
		rows := make([][]any, len(res.Rows))
		for i, row := range res.Rows {
			rows[i] = make([]any, len(indices))
			for j, n := range indices {
				rows[i][j] = row[n]
			}
		}
		res = engine.Result{Columns: cols, Rows: rows}
	}
	rawLimit := int64(0)
	if q.Has("raw_size") {
		rawLimit, e = strconv.ParseInt(q.Get("raw_size"), 10, 64)
		if e != nil || rawLimit < 0 {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("invalid raw_size"))
			return
		}
	}
	var data []byte
	if q.Has("arrow") { // Buffered C++ reader consumes one batch and advances by its row count.
		n := len(res.Rows)
		if rawLimit > 0 && n > 65536 {
			n = 65536
		}
		res.Rows = res.Rows[:n]
		for {
			data, e = wire.Arrow(res, max(1, n), true)
			if e != nil || rawLimit == 0 || int64(len(data)) <= rawLimit || n <= 1 {
				break
			}
			n = max(1, n/2)
			res.Rows = res.Rows[:n]
		}
	} else {
		data, e = wire.Protobuf(res)
	}
	if e != nil {
		fail(w, r, 400, "SerializationError", e)
		return
	}
	encoded, encoding, e := compress(data, r.Header.Get("Accept-Encoding"), q.Has("arrow"))
	if e != nil {
		fail(w, r, 400, "InvalidCompression", e)
		return
	}
	if encoding != "" {
		w.Header().Set("Content-Encoding", encoding)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
	w.Write(encoded)
}
func compress(b []byte, accept string, arrow bool) ([]byte, string, error) {
	var out bytes.Buffer
	var w io.WriteCloser
	encoding := ""
	for _, part := range strings.Split(accept, ",") {
		token := strings.TrimSpace(strings.Split(part, ";")[0])
		switch token {
		case "identity", "":
			return b, "", nil
		case "deflate":
			if arrow && strings.Contains(accept, ",") {
				continue
			}

			w = zlib.NewWriter(&out)
			encoding = token
		case "zstd":
			var err error
			w, err = zstd.NewWriter(&out, zstd.WithEncoderConcurrency(1))
			if err != nil {
				return nil, "", err
			}
			encoding = token
		case "x-snappy-framed":
			w = snappy.NewBufferedWriter(&out)
			encoding = token
		case "lz4_frame", "x-lz4-frame", "x-odps-lz4-frame":
			w = lz4.NewWriter(&out)
			encoding = token
		}
		if w != nil {
			break
		}
	}
	if w == nil {
		return nil, "", fmt.Errorf("unsupported Accept-Encoding %s", accept)
	}
	if _, e := w.Write(b); e != nil {
		return nil, "", e
	}
	if e := w.Close(); e != nil {
		return nil, "", e
	}
	return out.Bytes(), encoding, nil
}
