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

const Version = "1.1.0"

type Config struct {
	Project        string
	SessionTTL     time.Duration
	MaxSessions    int
	PublicEndpoint string
	QueryTimeout   time.Duration
	TestMode       bool
	Quotas         []string
	AuthMode       string
	Credentials    map[string]Credential
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
	// TaskType is the Instance/Job/Tasks element the instance was created from
	// ("SQL" for a one-shot statement, "SQLRT" for an interactive session).
	TaskType string
	// MCQA is non-nil for session instances; it holds the sub-query KV.
	MCQA *mcqaSession
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
	logKey    []byte
	faults    map[string]*FaultRule
}

func New(e *engine.Engine, c Config) *Server {
	if c.Project == "" {
		c.Project = "test_project"
	}
	if c.SessionTTL == 0 {
		c.SessionTTL = 30 * time.Minute
	}
	if c.MaxSessions == 0 {
		c.MaxSessions = 64
	}
	if c.QueryTimeout == 0 {
		c.QueryTimeout = 30 * time.Second
	}
	return &Server{logKey: []byte(id()), faults: map[string]*FaultRule{}, Engine: e, cfg: c, sessions: map[string]*session{}, instances: map[string]*instance{}, writes: map[string]*writeSession{}, storage: map[string]*storageSession{}, inflight: make(chan struct{}, 16)}
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
	if t := trace(r); t != nil {
		t.ErrorCode = code
	}
	message := e.Error()
	if len(message) > 600 {
		message = message[:600]
	}
	// `cached` is the direct session-result download, whose client parses a JSON tunnel
	// error rather than an XML one.
	if r.URL.Query().Has("downloads") || r.URL.Query().Has("downloadid") || r.URL.Query().Has("uploads") || r.URL.Query().Has("uploadid") || r.URL.Query().Has("cached") || strings.Contains(r.URL.Path, "/upserts") || strings.Contains(r.URL.Path, "/streams") || strings.Contains(r.URL.Path, "storage") {
		jsonResponse(w, status, map[string]string{"Code": code, "Message": message, "RequestId": w.Header().Get("x-odps-request-id")})
	} else {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		fmt.Fprintf(w, "<Error><Code>%s</Code><Message>%s</Message><RequestId>%s</RequestId></Error>", esc(code), esc(message), w.Header().Get("x-odps-request-id"))
	}
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t := s.requestTrace(r)
	r = withTrace(r, t)
	sw := &statusWriter{ResponseWriter: w}
	w = sw
	start := time.Now()
	requestID := id()
	w.Header().Set("x-odps-request-id", requestID)
	w.Header().Set("x-odps-tunnel-version", "5")
	w.Header().Set("Last-Modified", start.UTC().Format(http.TimeFormat))
	defer func() {
		if e := recover(); e != nil {
			slog.Error("request panic", "request_id", requestID)
			fail(w, r, 500, "InternalError", fmt.Errorf("request failed"))
		}
		t.Status = sw.status
		if t.Status == 0 {
			t.Status = 200
		}
		t.Elapsed = time.Since(start).Milliseconds()
		if enc := w.Header().Get("Content-Encoding"); enc != "" {
			t.Compression = enc
		}
		if t.Plane == "rest" {
			slog.Info("rest", "request_id", requestID, "action", t.Action, "object", t.Object, "project", t.Project, "status", t.Status, "error_code", t.ErrorCode, "elapsed_ms", t.Elapsed)
		} else if t.Action != "" {
			slog.Info("tunnel", "request_id", requestID, "action", t.Action, "download_id_hash", t.DownloadHash, "project", t.Project, "table", t.Table, "partition_present", t.Partition, "start", t.Start, "count", t.Count, "columns_count", t.Columns, "format", t.Format, "compression", t.Compression, "status", t.Status, "error_code", t.ErrorCode, "elapsed_ms", t.Elapsed, "quota", t.Quota)
		} else {
			slog.Info("request", "method", r.Method, "request_id", requestID, "status", t.Status, "elapsed_ms", t.Elapsed)
		}
	}()
	select {
	case s.inflight <- struct{}{}:
		defer func() { <-s.inflight }()
	default:
		w.Header().Set("Retry-After", "1")
		fail(w, r, 503, "ServiceUnavailable", fmt.Errorf("request capacity exceeded"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	r.Body = http.MaxBytesReader(w, r.Body, wire.MaxPayload)
	if strings.HasPrefix(r.URL.Path, "/__test/") {
		if r.URL.Path == "/__test/faults" || strings.HasPrefix(r.URL.Path, "/__test/faults/") {
			s.faultsAPI(w, r)
		} else {
			http.NotFound(w, r)
		}
		return
	}
	path := strings.Trim(r.URL.Path, "/")
	if strings.HasPrefix(path, "api/") {
		path = strings.TrimPrefix(path, "api/")
	}
	if path != "healthz" && path != "readyz" && !s.authenticate(w, r) {
		return
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
		jsonResponse(w, 200, map[string]any{"version": Version, "tunnel_download": []string{"create", "reload", "protobuf", "arrow", "complete"}, "compression": []string{"identity", "deflate", "zstd", "lz4_frame"}, "storage_v2": true, "storage_paths": []string{"/api/storage/v2", "/api/storage/v3"}, "tunnel_upload": []string{"protobuf", "arrow", "blocks", "stream", "upsert"}, "storage_write_modes": []string{"Batch", "BatchCompatible", "Streaming", "StreamingRealtime"}, "sql": "CREATE/DROP/INSERT/SELECT; static partitions; ODPS2 subset", "resources": []string{"file", "jar", "py", "archive", "table-metadata"}, "functions": []string{"java-udf-metadata", "sql-function-metadata", "embedded-function-metadata"}, "mcqa": []string{"sqlrt-session", "subquery-query", "subquery-result", "subquery-cancel", "session-stop", "subquery-instance-tunnel", "session-select-only"}, "unsupported": []string{"volume-resources", "sql-udf-execution", "volumes", "mcqa-named-session-attach", "mcqa-v2-maxqa", "catalogapi"}, "auth": s.authDescription(), "test_faults": s.cfg.TestMode, "quotas": append([]string{"default"}, s.cfg.Quotas...)})
		return
	}
	if path == "logview/host" {
		s.logView(w, r)
		return
	}
	if path == "connection/mcqa" {
		s.mcqaConnection(w, r)
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[0] != "projects" {
		fail(w, r, 404, "UnsupportedOperation", fmt.Errorf("unsupported endpoint"))
		return
	}
	project := parts[1]
	if !s.checkProject(w, r, project) {
		return
	}
	if !s.beforeProjectRequest(w, r) {
		return
	}
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
	if len(rest) >= 1 && rest[0] == "resources" {
		s.resources(w, r, project, schema, rest[1:])
		return
	}
	if len(rest) >= 2 && rest[0] == "registration" && rest[1] == "functions" {
		s.functions(w, r, project, schema, rest[2:])
		return
	}
	if len(rest) == 1 && rest[0] == "authorization" {
		s.signBearerToken(w, r, project)
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
			s.listTables(w, r, project, schema, tables)
			return
		}
	}
	fail(w, r, 404, "UnsupportedOperation", fmt.Errorf("unsupported endpoint"))
}
func tableXML(p, s string, t engine.Table) string {
	reservedMap := map[string]any{"Transactional": t.Properties["transactional"] == "true", "PrimaryKey": append([]string{}, t.PrimaryKeys...), "schema_version": "1"}
	if len(t.PrimaryKeys) > 0 {
		reservedMap["ClusterType"] = "hash"
		reservedMap["BucketNum"] = 1
		reservedMap["ClusterCols"] = t.PrimaryKeys
	}
	reserved, _ := json.Marshal(reservedMap)
	lifecycle, _ := strconv.ParseInt(t.Properties["lifecycle"], 10, 64)
	b, _ := json.Marshal(map[string]any{"columns": asArray(t.Columns), "partitionKeys": asArray(t.Partitions), "Reserved": string(reserved), "createTime": t.Created, "lastDDLTime": t.Created, "lastModifiedTime": t.Created, "lifecycle": lifecycle})
	return "<Table><Name>" + esc(t.Name) + "</Name><TableId>" + esc(t.ID) + "</TableId><Project>" + esc(p) + "</Project><SchemaName>" + esc(s) + "</SchemaName><Owner>emulator</Owner><Type>MANAGED_TABLE</Type><Schema>" + esc(string(b)) + "</Schema><Comment>" + esc(t.Properties["comment"]) + "</Comment></Table>"
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
		s.partitionMetadata(w, r, p, sc, table)
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
					SQLRT []struct {
						Name   string `xml:"Name"`
						Config struct {
							Properties []mcqaProperty `xml:"Property"`
						} `xml:"Config"`
					} `xml:"SQLRT"`
				} `xml:"Tasks"`
			} `xml:"Job"`
		}
		if e := xml.NewDecoder(r.Body).Decode(&req); e != nil {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("malformed instance XML"))
			return
		}
		if len(req.Job.Tasks.SQLRT) > 0 {
			if len(req.Job.Tasks.SQL) > 0 {
				fail(w, r, 400, "InvalidParameter", fmt.Errorf("one task type per instance"))
				return
			}
			rt := req.Job.Tasks.SQLRT[0]
			s.mcqaCreate(w, r, p, sc, rt.Name, mcqaTaskSettings(rt.Config.Properties))
			return
		}
		if len(req.Job.Tasks.SQL) != 1 {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("one SQL task in Instance/Job/Tasks required"))
			return
		}
		task := req.Job.Tasks.SQL[0]
		if task.Name == "" {
			task.Name = "AnonymousSQLTask"
		}
		i := &instance{Project: p, Schema: sc, ID: id(), Name: task.Name, Query: task.Query, Status: "Success", Created: time.Now()}
		// Reserve capacity before executing SQL: a rejected INSERT must not commit.
		pending := *i
		pending.Status = "Running"
		if !s.reserveInstance(&pending) {
			fail(w, r, 429, "ResourceLimit", fmt.Errorf("instance limit"))
			return
		}
		defer func() {
			s.mu.Lock()
			if s.instances[i.ID] == &pending {
				delete(s.instances, i.ID)
			}
			s.mu.Unlock()
		}()
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
		s.publishInstance(i)
		w.Header().Set("Location", "/projects/"+url.PathEscape(p)+"/instances/"+i.ID)
		w.WriteHeader(201)
		return
	}
	if len(rest) == 1 && (r.URL.Query().Has("downloads") || r.URL.Query().Has("downloadid")) {
		s.instanceDownload(w, r, p, sc, rest[0])
		return
	}
	// A session sub query is read straight off the instance: the Java SDK's direct
	// download arrives as GET ?data&cached&taskname=.., with no download id to look up.
	if len(rest) == 1 && r.Method == http.MethodGet && r.URL.Query().Has("data") &&
		(r.URL.Query().Has("cached") || r.URL.Query().Has("taskname")) {
		s.mcqaDirectDownload(w, r, p, sc, rest[0])
		return
	}
	if len(rest) != 1 || r.Method != "GET" && r.Method != "PUT" {
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
	if r.URL.Query().Has("info") {
		s.mcqaInfo(w, r, i)
		return
	}
	if r.Method == "PUT" {
		if i.MCQA == nil {
			fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("only SQLRT session instances can be stopped"))
			return
		}
		s.mcqaStop(w, r, i)
		return
	}
	s.mu.Lock()
	expired := s.mcqaIdle(i)
	if expired {
		delete(s.instances, i.ID)
	}
	s.mu.Unlock()
	if expired {
		fail(w, r, 404, "NoSuchInstance", fmt.Errorf("unknown instance"))
		return
	}
	if i.MCQA == nil && (i.Status == "Running" || time.Since(i.Created) > s.cfg.SessionTTL) {
		fail(w, r, 404, "NoSuchInstance", fmt.Errorf("unknown instance"))
		return
	}
	if i.MCQA != nil {
		s.mcqaInstanceXML(w, i)
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

// Instance results retain both typed rows and the SQL API's CSV representation.
// A single bounded result can expand when CSV formats BINARY values, hence this
// budget is larger than the engine's 64 MiB result limit. Eviction happens after
// execution without rejecting an already committed SQL operation.
const maxInstanceBytes int64 = 512 << 20

func instanceBytes(i *instance) int64 {
	return i.Data.Bytes + i.MCQA.bytes() + int64(len(i.Query)+len(i.Output))
}

func (s *Server) reserveInstance(i *instance) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, old := range s.instances {
		if old.MCQA != nil {
			// Sessions age out on idle time, not on creation time.
			if s.mcqaIdle(old) {
				delete(s.instances, key)
			}
			continue
		}
		if old.Status != "Running" && time.Since(old.Created) > s.cfg.SessionTTL {
			delete(s.instances, key)
		}
	}
	if len(s.instances) >= 10000 {
		return false
	}
	s.instances[i.ID] = i
	return true
}

func (s *Server) publishInstance(i *instance) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for key, old := range s.instances {
		if key != i.ID {
			total += instanceBytes(old)
		}
	}
	for total+instanceBytes(i) > maxInstanceBytes {
		var oldest *instance
		var oldestKey string
		for key, old := range s.instances {
			if key != i.ID && old.Status != "Running" && (oldest == nil || old.Created.Before(oldest.Created)) {
				oldest, oldestKey = old, key
			}
		}
		if oldest == nil {
			break
		}
		total -= instanceBytes(oldest)
		delete(s.instances, oldestKey)
	}
	s.instances[i.ID] = i
}
func (s *Server) createDownload(w http.ResponseWriter, r *http.Request, p, sc, t string) {
	part, err := engine.ParsePartition(r.URL.Query().Get("partition"))
	if err != nil {
		fail(w, r, 400, "InvalidPartitionSpec", err)
		return
	}
	data, meta, err := s.Engine.Snapshot(r.Context(), p, sc, t, part)
	if err != nil {
		code, status := "InvalidParameter", 400
		if strings.HasPrefix(err.Error(), "NoSuchTable:") {
			code, status = "NoSuchTable", 404
		}
		if strings.HasPrefix(err.Error(), "NoSuchPartition:") {
			code, status = "NoSuchPartition", 404
		}
		if strings.HasPrefix(err.Error(), "InvalidPartition:") {
			code = "InvalidPartitionSpec"
		}
		fail(w, r, status, code, err)
		return
	}
	partJSON, _ := json.Marshal(part)
	sess := &session{ID: id(), Project: p, Schema: sc, Table: strings.ToLower(t), Partition: string(partJSON), Quota: trace(r).Quota, Data: data, Meta: meta, Created: time.Now()}
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
	if t := trace(r); t != nil {
		t.DownloadHash = s.hashSession(sess.ID)
	}
	jsonResponse(w, 200, sessionJSON(sess))
}

// asArray substitutes an empty slice for a nil one. encoding/json writes a nil slice as
// `null`, and the Java SDK reads schema members with JsonObject#getAsJsonArray, which
// throws "Not a JSON Array: null" — reported to the caller as a TunnelException with
// nothing to suggest the real cause. A result with no columns is normal: every DDL and
// every INSERT is one, and the JDBC driver opens a download session for them too.
func asArray[T any](v []T) []T {
	if v == nil {
		return []T{}
	}
	return v
}

func sessionJSON(s *session) map[string]any {
	return map[string]any{"DownloadID": s.ID, "RecordCount": len(s.Data.Rows), "Status": "normal", "Owner": "emulator", "Initiated": s.Created.UTC().Format(time.RFC3339), "Schema": map[string]any{"columns": asArray(s.Meta.Columns), "partitionKeys": asArray(s.Meta.Partitions), "IsVirtualView": false}, "QuotaName": s.Quota}
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
	res = faultRows(r, res)
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
			batchRows := min(4096, max(1, n))
			if rawLimit > 0 {
				batchRows = max(1, n)
			}
			data, e = wire.TunnelArrow(res, batchRows, arrowEncoding(r.Header.Get("Accept-Encoding")))
			if e != nil || rawLimit == 0 || int64(len(data)) <= rawLimit || n <= 1 {
				break
			}
			n = max(1, n/2)
			res.Rows = res.Rows[:n]
		}
	} else {
		data, e = wire.Protobuf(res)
		if tr := trace(r); tr != nil && tr.fault != nil && tr.fault.Type == "disconnect_after_rows" {
			data, e = wire.ProtobufPrefix(res)
		}
	}
	if e != nil {
		fail(w, r, 400, "SerializationError", e)
		return
	}
	if t := trace(r); t != nil && t.fault != nil {
		switch t.fault.Type {
		case "empty_arrow_batch":
			data, e = wire.EmptyTunnelArrow(res)
		case "malformed_arrow":
			data = wire.ArrowChunk([]byte{255, 255, 255, 255, 7, 0, 0, 0, 1, 2, 3})
		case "malformed_protobuf":
			data = []byte{0x0f}
		case "crc_mismatch":
			if len(data) > 0 {
				data[len(data)-1] ^= 0x01
			}
		}
		if e != nil {
			fail(w, r, 500, "InternalError", e)
			return
		}
	}
	var encoded []byte
	var encoding string
	if q.Has("arrow") && arrowEncoding(r.Header.Get("Accept-Encoding")) != "" {
		encoded, encoding = data, arrowEncoding(r.Header.Get("Accept-Encoding"))
	} else {
		encoded, encoding, e = compress(data, r.Header.Get("Accept-Encoding"), q.Has("arrow"))
	}
	if e != nil {
		fail(w, r, 400, "InvalidCompression", e)
		return
	}
	if encoding != "" {
		w.Header().Set("Content-Encoding", encoding)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
	if t := trace(r); t != nil && t.fault != nil {
		e := t.fault
		switch e.Type {
		case "disconnect_after_bytes":
			n := min(e.Bytes, len(encoded))
			w.Write(encoded[:n])
			return
		case "disconnect_after_rows":
			w.Header().Set("Content-Length", strconv.Itoa(len(encoded)+1))
		}
	}
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

// Arrow codecs use IPC buffer compression; other codecs retain HTTP wrapping.
func arrowEncoding(accept string) string {
	for _, part := range strings.Split(accept, ",") {
		token := strings.TrimSpace(strings.Split(part, ";")[0])
		switch token {
		case "", "identity", "x-snappy-framed", "x-odps-lz4-frame":
			return ""
		case "deflate":
			if !strings.Contains(accept, ",") {
				return ""
			}
		case "zstd":
			return "zstd"
		case "lz4_frame", "x-lz4-frame":
			return token
		}
	}
	return ""
}
func (s *Server) checkProject(w http.ResponseWriter, r *http.Request, project string) bool {
	if project == s.cfg.Project {
		return true
	}
	exists, err := s.Engine.HasProject(r.Context(), project)
	if err != nil {
		fail(w, r, 500, "InternalError", err)
		return false
	}
	if !exists {
		fail(w, r, 404, "NoSuchProject", fmt.Errorf("The specified project name does not exist."))
		return false
	}
	return true
}
