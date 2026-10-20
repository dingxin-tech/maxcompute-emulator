package server

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

func schemaXML(project string, schema engine.Schema) string {
	return "<Schema><Name>" + esc(schema.Name) + "</Name><Project>" + esc(project) + "</Project><Type>MANAGED</Type><Comment>" + esc(schema.Comment) + "</Comment><Owner>emulator</Owner></Schema>"
}

func (s *Server) schemas(w http.ResponseWriter, r *http.Request, project string, rest []string) {
	if len(rest) == 0 && r.Method == "POST" {
		var request struct {
			XMLName     xml.Name `xml:"Schema"`
			Name        string   `xml:"Name"`
			Comment     string   `xml:"Comment"`
			Type        string   `xml:"Type"`
			IfNotExists bool     `xml:"IfNotExists"`
		}
		if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
			fail(w, r, 400, "SchemaParseError", fmt.Errorf("invalid schema XML"))
			return
		}
		if request.Type != "" && !strings.EqualFold(request.Type, "MANAGED") {
			fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("only managed schemas are supported"))
			return
		}
		err := s.Engine.CreateSchema(r.Context(), project, request.Name, request.Comment, request.IfNotExists)
		if errors.Is(err, engine.ErrSchemaExists) {
			fail(w, r, 409, "ObjectAlreadyExists", err)
			return
		}
		if err != nil {
			fail(w, r, 400, "InvalidParameter", err)
			return
		}
		w.WriteHeader(http.StatusCreated)
		return
	}
	if len(rest) == 1 && r.Method == "DELETE" {
		err := s.Engine.DeleteSchema(r.Context(), project, rest[0])
		if errors.Is(err, engine.ErrSchemaMissing) {
			fail(w, r, 404, "NoSuchObject", err)
			return
		}
		if err != nil {
			fail(w, r, 400, "InvalidOperation", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != "GET" {
		fail(w, r, 400, "UnsupportedOperation", fmt.Errorf("unsupported schema operation"))
		return
	}
	schemas, err := s.Engine.Schemas(r.Context(), project)
	if err != nil {
		fail(w, r, 500, "InternalError", err)
		return
	}
	if len(rest) == 1 {
		for _, schema := range schemas {
			if schema.Name == strings.ToLower(rest[0]) {
				date := time.Unix(schema.Created, 0).UTC().Format(http.TimeFormat)
				w.Header().Set("x-odps-creation-time", date)
				w.Header().Set("Last-Modified", date)
				xmlResponse(w, schemaXML(project, schema))
				return
			}
		}
		fail(w, r, 404, "NoSuchObject", engine.ErrSchemaMissing)
		return
	}
	q := r.URL.Query()
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
	more := false
	for _, schema := range schemas {
		if schema.Name <= q.Get("marker") || !strings.HasPrefix(schema.Name, q.Get("name")) || (q.Get("owner") != "" && q.Get("owner") != "emulator") || (q.Get("type") != "" && !strings.EqualFold(strings.TrimSpace(q.Get("type")), "MANAGED")) {
			continue
		}
		if count == limit {
			more = true
			break
		}
		body.WriteString(schemaXML(project, schema))
		marker = schema.Name
		count++
	}
	if !more {
		marker = ""
	}
	xmlResponse(w, fmt.Sprintf("<Schemas><Marker>%s</Marker><MaxItems>%d</MaxItems>%s</Schemas>", esc(marker), limit, body.String()))
}
