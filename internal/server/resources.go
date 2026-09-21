package server

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

// Resources and registered functions are metadata-plane objects: the Java SDK
// (Resources, Functions), PyODPS (odps.models.resources, odps.models.functions)
// and the Go SDK all address them below the project (or project/schema) node,
// resources at /resources and functions at /registration/functions.
//
// Resource payloads are stored as opaque bytes; chunked partial uploads
// (rIsPart/rOpMerge) and volume-backed resources are out of scope and rejected
// with UnsupportedOperation instead of silently succeeding.

func rfc822(millis int64) string {
	return time.UnixMilli(millis).UTC().Format(http.TimeFormat)
}

// resourceFailure maps engine error prefixes to ODPS codes. 404 is what makes
// the Java SDK raise NoSuchObjectException, which SDK exists() helpers rely on.
func resourceFailure(w http.ResponseWriter, r *http.Request, e error) {
	msg := e.Error()
	code, status := "InvalidParameter", 400
	switch prefix, _, cut := strings.Cut(msg, ":"); {
	case strings.HasPrefix(msg, "NoSuchResource:"), strings.HasPrefix(msg, "NoSuchFunction:"):
		code, status = "NoSuchObject", 404
	case cut && prefix == "ResourceAlreadyExists", cut && prefix == "FunctionAlreadyExists":
		code = prefix
	case cut && prefix == "InvalidResourceType", cut && prefix == "ResourceOverSize", cut && prefix == "UnsupportedOperation":
		code = prefix
	}
	fail(w, r, status, code, e)
}

func listParams(r *http.Request) (prefix, marker string, limit int, err error) {
	q := r.URL.Query()
	prefix, marker = q.Get("name"), q.Get("marker")
	limit = 1000
	if q.Get("maxitems") != "" {
		limit, err = strconv.Atoi(q.Get("maxitems"))
		if err != nil || limit < 1 || limit > 10000 {
			err = fmt.Errorf("maxitems must be 1..10000")
		}
	}
	return
}

func (s *Server) resources(w http.ResponseWriter, r *http.Request, p, sc string, rest []string) {
	if len(rest) == 0 {
		switch r.Method {
		case "GET":
			s.listResources(w, r, p, sc)
		case "POST":
			s.putResource(w, r, p, sc, "", engine.ResourceCreate)
		default:
			fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("unsupported resources operation %s", r.Method))
		}
		return
	}
	if len(rest) != 1 || rest[0] == "" {
		fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("unsupported resource path"))
		return
	}
	name := rest[0]
	switch r.Method {
	case "GET":
		if r.URL.Query().Has("meta") {
			s.resourceMeta(w, r, p, sc, name)
			return
		}
		s.resourceContent(w, r, p, sc, name)
	case "PUT", "POST":
		s.putResource(w, r, p, sc, name, engine.ResourceReplace)
	case "DELETE":
		if e := s.Engine.DeleteResource(r.Context(), p, sc, name); e != nil {
			resourceFailure(w, r, e)
			return
		}
		w.WriteHeader(200)
	default:
		fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("unsupported resource operation %s", r.Method))
	}
}

func (s *Server) listResources(w http.ResponseWriter, r *http.Request, p, sc string) {
	prefix, marker, limit, err := listParams(r)
	if err != nil {
		fail(w, r, 400, "InvalidParameter", err)
		return
	}
	typeFilter := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("type")))
	all, err := s.Engine.Resources(r.Context(), p, sc)
	if err != nil {
		fail(w, r, 500, "InternalError", err)
		return
	}
	var body strings.Builder
	marker, prefix, count, more := strings.ToLower(marker), strings.ToLower(prefix), 0, false
	for _, res := range all {
		key := strings.ToLower(res.Name)
		if !strings.HasPrefix(key, prefix) || key <= marker || (typeFilter != "" && typeFilter != "ALL" && typeFilter != res.Type) {
			continue
		}
		if count == limit {
			more = true
			break
		}
		body.WriteString(resourceXML(sc, res))
		marker, count = key, count+1
	}
	if !more {
		marker = ""
	}
	xmlResponse(w, fmt.Sprintf("<Resources><Marker>%s</Marker><MaxItems>%d</MaxItems>%s</Resources>", esc(marker), limit, body.String()))
}

func resourceXML(sc string, res engine.Resource) string {
	b := strings.Builder{}
	fmt.Fprintf(&b, "<Resource><Name>%s</Name><ResourceType>%s</ResourceType><Owner>emulator</Owner>", esc(res.Name), esc(res.Type))
	if sc != "" {
		fmt.Fprintf(&b, "<SchemaName>%s</SchemaName>", esc(sc))
	}
	fmt.Fprintf(&b, "<Comment>%s</Comment>", esc(res.Comment))
	fmt.Fprintf(&b, "<CreationTime>%s</CreationTime><LastModifiedTime>%s</LastModifiedTime>", rfc822(res.Created), rfc822(res.Updated))
	b.WriteString("<LastUpdator>emulator</LastUpdator>")
	fmt.Fprintf(&b, "<ResourceSize>%d</ResourceSize>", res.Size)
	if res.IsTemp {
		b.WriteString("<IsTempResource>true</IsTempResource>")
	}
	if res.TableName != "" {
		fmt.Fprintf(&b, "<TableName>%s</TableName>", esc(res.TableName))
	}
	b.WriteString("</Resource>")
	return b.String()
}

// putResource accepts the three upload shapes the public SDKs use: a single
// payload (PyODPS), a part upload plus merge (Java SDK Resources.create, which
// always chunks), and a metadata-only TABLE resource.
func (s *Server) putResource(w http.ResponseWriter, r *http.Request, p, sc, name string, mode engine.ResourceMode) {
	q := r.URL.Query()
	if r.Header.Get("x-odps-copy-file-source") != "" {
		fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("volume-backed resources are not supported"))
		return
	}
	if q.Has("rIsPart") && q.Has("rOpMerge") {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("rIsPart and rOpMerge are mutually exclusive"))
		return
	}
	if q.Has("rIsPart") {
		s.putResourcePart(w, r, p, sc, name)
		return
	}
	if q.Has("rOpMerge") {
		s.mergeResourceParts(w, r, p, sc, name, mode)
		return
	}
	target, ok := s.resourceTarget(w, r, name)
	if !ok {
		return
	}
	content, err := io.ReadAll(r.Body)
	if err != nil {
		fail(w, r, 400, "InvalidParameter", err)
		return
	}
	s.storeResource(w, r, p, sc, target, r.Header.Get("x-odps-resource-type"), r.Header.Get("x-odps-comment"),
		strings.EqualFold(r.Header.Get("x-odps-resource-istemp"), "true"), r.Header.Get("x-odps-copy-table-source"), content, mode)
}

// resourceTarget reconciles the x-odps-resource-name header with the path
// segment; both SDKs send the header, and PyODPS sends the path on overwrite.
func (s *Server) resourceTarget(w http.ResponseWriter, r *http.Request, name string) (string, bool) {
	header := r.Header.Get("x-odps-resource-name")
	if header == "" {
		if name == "" {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("x-odps-resource-name or a resource path segment is required"))
			return "", false
		}
		return name, true
	}
	if name != "" && !strings.EqualFold(header, name) {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("resource name %q does not match the path", header))
		return "", false
	}
	return header, true
}

func (s *Server) storeResource(w http.ResponseWriter, r *http.Request, p, sc, name, kind, comment string, isTemp bool, tableSource string, content []byte, mode engine.ResourceMode) {
	res, err := s.Engine.PutResource(r.Context(), p, sc, name, kind, comment, isTemp, tableSource, content, mode)
	if err != nil {
		resourceFailure(w, r, err)
		return
	}
	w.Header().Set("Location", "/projects/"+url.PathEscape(p)+"/resources/"+url.PathEscape(res.Name))
	if mode == engine.ResourceCreate {
		w.WriteHeader(201)
		return
	}
	w.WriteHeader(200)
}

// putResourcePart stores one chunk of a chunked upload under a deterministic
// temporary name. SDK retries re-POST the same name, so parts upsert.
func (s *Server) putResourcePart(w http.ResponseWriter, r *http.Request, p, sc, name string) {
	if !strings.EqualFold(r.Header.Get("x-odps-resource-istemp"), "true") {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("part uploads must set x-odps-resource-istemp"))
		return
	}
	target, ok := s.resourceTarget(w, r, name)
	if !ok {
		return
	}
	content, err := io.ReadAll(r.Body)
	if err != nil {
		fail(w, r, 400, "InvalidParameter", err)
		return
	}
	kind := r.Header.Get("x-odps-resource-type")
	if kind == "" {
		kind = "file"
	}
	s.storeResource(w, r, p, sc, target, kind, r.Header.Get("x-odps-comment"), true, "", content, engine.ResourceUpsert)
}

// mergeResourceParts assembles the declared parts into the final resource. The
// body is "<md5-hex>|<part>[,<part>...]"; the digest and the declared total
// bytes are both verified before the payload becomes visible.
func (s *Server) mergeResourceParts(w http.ResponseWriter, r *http.Request, p, sc, name string, mode engine.ResourceMode) {
	target, ok := s.resourceTarget(w, r, name)
	if !ok {
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fail(w, r, 400, "InvalidParameter", err)
		return
	}
	digest, list, found := strings.Cut(string(body), "|")
	if !found {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("merge body must be <md5>|<part>[,<part>...]"))
		return
	}
	parts := []string{}
	for _, part := range strings.Split(list, ",") {
		if part = strings.TrimSpace(part); part != "" {
			parts = append(parts, part)
		}
	}
	if len(parts) == 0 {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("merge requires at least one part resource"))
		return
	}
	merged := []byte{}
	for _, part := range parts {
		_, content, e := s.Engine.ResourceContent(r.Context(), p, sc, part)
		if e != nil {
			resourceFailure(w, r, e)
			return
		}
		merged = append(merged, content...)
	}
	if size := r.Header.Get("x-odps-resource-merge-total-bytes"); size != "" {
		declared, e := strconv.ParseInt(size, 10, 64)
		if e != nil || declared != int64(len(merged)) {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("x-odps-resource-merge-total-bytes does not match the merged payload"))
			return
		}
	}
	sum := md5.Sum(merged)
	if !strings.EqualFold(hex.EncodeToString(sum[:]), strings.TrimSpace(digest)) {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("merged payload does not match MD5 %s", strings.TrimSpace(digest)))
		return
	}
	s.storeResource(w, r, p, sc, target, r.Header.Get("x-odps-resource-type"), r.Header.Get("x-odps-comment"),
		strings.EqualFold(r.Header.Get("x-odps-resource-istemp"), "true"), "", merged, mode)
	if w.Header().Get("Location") == "" {
		return
	}
	for _, part := range parts {
		s.Engine.DeleteResource(r.Context(), p, sc, part)
	}
}

func (s *Server) resourceMeta(w http.ResponseWriter, r *http.Request, p, sc, name string) {
	res, err := s.Engine.Resource(r.Context(), p, sc, name)
	if err != nil {
		resourceFailure(w, r, err)
		return
	}
	h := w.Header()
	h.Set("x-odps-owner", "emulator")
	h.Set("x-odps-resource-type", res.Type)
	h.Set("x-odps-updator", "emulator")
	h.Set("x-odps-resource-size", strconv.FormatInt(res.Size, 10))
	h.Set("x-odps-creation-time", rfc822(res.Created))
	h.Set("Last-Modified", rfc822(res.Updated))
	h.Set("x-odps-resource-istemp", strconv.FormatBool(res.IsTemp))
	h.Set("schema-name", sc)
	if res.MD5 != "" {
		h.Set("Content-MD5", res.MD5)
	}
	h.Set("Content-Type", "application/xml")
	if res.Comment != "" {
		h.Set("x-odps-comment", res.Comment)
	}
	if res.TableName != "" {
		h.Set("x-odps-copy-table-source", res.TableName)
	}
	w.WriteHeader(200)
}

func (s *Server) resourceContent(w http.ResponseWriter, r *http.Request, p, sc, name string) {
	res, content, err := s.Engine.ResourceContent(r.Context(), p, sc, name)
	if err != nil {
		resourceFailure(w, r, err)
		return
	}
	start, count, size := 0, len(content), len(content)
	if v := r.URL.Query().Get("rOffset"); v != "" {
		if start, err = strconv.Atoi(v); err != nil || start < 0 || start > size {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("rOffset must be within 0..%d", size))
			return
		}
	}
	if v := r.URL.Query().Get("rSize"); v != "" {
		if count, err = strconv.Atoi(v); err != nil || count < 0 {
			fail(w, r, 400, "InvalidParameter", fmt.Errorf("rSize must be a non-negative integer"))
			return
		}
		if count > size-start {
			count = size - start
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment;filename="+strconv.Quote(res.Name))
	w.Header().Set("x-odps-resource-size", strconv.Itoa(size))
	w.Header().Set("x-odps-resource-has-remaining", strconv.FormatBool(start+count < size))
	w.Header().Set("Last-Modified", rfc822(res.Updated))
	w.WriteHeader(200)
	w.Write(content[start : start+count])
}

// functionXML is the wire shape shared by the Java SDK FunctionModel and the
// PyODPS Function serializer: the alias travels in the Alias element, with
// referenced resources as ResourceName children. Name is accepted as an alias
// for clients that use the older element.
type functionXML struct {
	Alias        string `xml:"Alias"`
	Name         string `xml:"Name"`
	SchemaName   string `xml:"SchemaName"`
	Owner        string `xml:"Owner"`
	CreationTime string `xml:"CreationTime"`
	ClassType    string `xml:"ClassType"`
	Resources    struct {
		Items []string `xml:"ResourceName"`
	} `xml:"Resources"`
	IsSqlFunction      string `xml:"IsSqlFunction"`
	SqlDefinitionText  string `xml:"SqlDefinitionText"`
	IsEmbeddedFunction string `xml:"IsEmbeddedFunction"`
	ProgramLanguage    string `xml:"ProgramLanguage"`
	Code               string `xml:"Code"`
	FileName           string `xml:"FileName"`
}

func (f *functionXML) toModel(owner string) engine.Function {
	sql, _ := strconv.ParseBool(strings.ToLower(strings.TrimSpace(f.IsSqlFunction)))
	embedded, _ := strconv.ParseBool(strings.ToLower(strings.TrimSpace(f.IsEmbeddedFunction)))
	name := f.Alias
	if name == "" {
		name = f.Name
	}
	return engine.Function{Name: name, Owner: owner, ClassType: strings.TrimSpace(f.ClassType),
		Resources: f.Resources.Items, SQLFunction: sql, SQLText: f.SqlDefinitionText,
		Embedded: embedded, Language: f.ProgramLanguage, Code: f.Code, FileName: f.FileName}
}

func functionXMLText(sc string, f engine.Function) string {
	b := strings.Builder{}
	fmt.Fprintf(&b, "<Function><Alias>%s</Alias>", esc(f.Name))
	if sc != "" {
		fmt.Fprintf(&b, "<SchemaName>%s</SchemaName>", esc(sc))
	}
	fmt.Fprintf(&b, "<Owner>%s</Owner><CreationTime>%s</CreationTime>", esc(orDefault(f.Owner, "emulator")), rfc822(f.Created))
	if !f.SQLFunction && !f.Embedded {
		fmt.Fprintf(&b, "<ClassType>%s</ClassType>", esc(f.ClassType))
	}
	if len(f.Resources) > 0 {
		b.WriteString("<Resources>")
		for _, res := range f.Resources {
			fmt.Fprintf(&b, "<ResourceName>%s</ResourceName>", esc(res))
		}
		b.WriteString("</Resources>")
	}
	if f.SQLFunction {
		b.WriteString("<IsSqlFunction>true</IsSqlFunction>")
		fmt.Fprintf(&b, "<SqlDefinitionText>%s</SqlDefinitionText>", esc(f.SQLText))
	}
	if f.Embedded {
		b.WriteString("<IsEmbeddedFunction>true</IsEmbeddedFunction>")
		fmt.Fprintf(&b, "<ProgramLanguage>%s</ProgramLanguage><Code>%s</Code><FileName>%s</FileName>", esc(f.Language), esc(f.Code), esc(f.FileName))
	}
	b.WriteString("</Function>")
	return b.String()
}

func orDefault(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func (s *Server) functions(w http.ResponseWriter, r *http.Request, p, sc string, rest []string) {
	if len(rest) == 0 {
		switch r.Method {
		case "GET":
			s.listFunctions(w, r, p, sc)
		case "POST":
			s.putFunction(w, r, p, sc, "", false)
		default:
			fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("unsupported functions operation %s", r.Method))
		}
		return
	}
	if len(rest) != 1 || rest[0] == "" {
		fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("unsupported function path"))
		return
	}
	name := rest[0]
	switch r.Method {
	case "GET":
		f, err := s.Engine.Function(r.Context(), p, sc, name)
		if err != nil {
			resourceFailure(w, r, err)
			return
		}
		xmlResponse(w, functionXMLText(sc, f))
	case "PUT", "POST":
		s.putFunction(w, r, p, sc, name, true)
	case "DELETE":
		if e := s.Engine.DeleteFunction(r.Context(), p, sc, name); e != nil {
			resourceFailure(w, r, e)
			return
		}
		w.WriteHeader(200)
	default:
		fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("unsupported function operation %s", r.Method))
	}
}

func (s *Server) listFunctions(w http.ResponseWriter, r *http.Request, p, sc string) {
	prefix, marker, limit, err := listParams(r)
	if err != nil {
		fail(w, r, 400, "InvalidParameter", err)
		return
	}
	all, err := s.Engine.Functions(r.Context(), p, sc)
	if err != nil {
		fail(w, r, 500, "InternalError", err)
		return
	}
	var body strings.Builder
	marker, count, more := strings.ToLower(marker), 0, false
	for _, f := range all {
		key := strings.ToLower(f.Name)
		if !strings.HasPrefix(key, strings.ToLower(prefix)) || key <= marker {
			continue
		}
		if count == limit {
			more = true
			break
		}
		body.WriteString(functionXMLText(sc, f))
		marker, count = key, count+1
	}
	if !more {
		marker = ""
	}
	xmlResponse(w, fmt.Sprintf("<Functions><Marker>%s</Marker><MaxItems>%d</MaxItems>%s</Functions>", esc(marker), limit, body.String()))
}

func (s *Server) putFunction(w http.ResponseWriter, r *http.Request, p, sc, name string, overwrite bool) {
	var req functionXML
	if e := xml.NewDecoder(r.Body).Decode(&req); e != nil && e != io.EOF {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("Function XML body required: %v", e))
		return
	}
	model := req.toModel(orDefault(req.Owner, "emulator"))
	if model.Name == "" {
		model.Name = name
	} else if name != "" && !strings.EqualFold(model.Name, name) {
		fail(w, r, 400, "InvalidParameter", fmt.Errorf("function alias %q does not match the path", model.Name))
		return
	}
	if model.Owner == "" {
		model.Owner = "emulator"
	}
	created, e := s.Engine.PutFunction(r.Context(), p, sc, model, overwrite)
	if e != nil {
		resourceFailure(w, r, e)
		return
	}
	w.Header().Set("Location", "/projects/"+url.PathEscape(p)+"/registration/functions/"+url.PathEscape(created.Name))
	if overwrite {
		w.WriteHeader(200)
		return
	}
	w.WriteHeader(201)
}
