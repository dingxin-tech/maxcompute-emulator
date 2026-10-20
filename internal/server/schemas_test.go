package server

import (
	"context"
	"strings"
	"testing"
)

func TestSchemaREST(t *testing.T) {
	s, h := fixture(t, Config{})
	for _, name := range []string{"alpha", "another"} {
		code, body := sendBody(t, h, "POST", "/projects/p/schemas", []byte("<Schema><Name>"+name+"</Name><Comment>test</Comment></Schema>"), nil)
		if code != 201 {
			t.Fatalf("create: %d %s", code, body)
		}
	}
	code, body := sendBody(t, h, "POST", "/projects/p/schemas", []byte("<Schema><Name>alpha</Name></Schema>"), nil)
	if code != 409 || !strings.Contains(string(body), "ObjectAlreadyExists") {
		t.Fatalf("duplicate: %d %s", code, body)
	}
	code, body = sendBody(t, h, "POST", "/projects/p/schemas", []byte("<Schema><Name>alpha</Name><IfNotExists>true</IfNotExists></Schema>"), nil)
	if code != 201 {
		t.Fatalf("if-not-exists: %d %s", code, body)
	}
	code, header, body := request(t, h, "GET", "/projects/p/schemas/alpha", "")
	if code != 200 || header.Get("Last-Modified") == "" || !strings.Contains(string(body), "<Comment>test</Comment>") {
		t.Fatalf("get: %d %s", code, body)
	}
	code, body = sendBody(t, h, "GET", "/projects/p/schemas?name=a&maxitems=1", nil, nil)
	if code != 200 || !strings.Contains(string(body), "<Marker>alpha</Marker>") || strings.Contains(string(body), "<Name>another</Name>") {
		t.Fatalf("first page: %d %s", code, body)
	}
	code, body = sendBody(t, h, "GET", "/projects/p/schemas?name=a&maxitems=1&marker=alpha", nil, nil)
	if code != 200 || !strings.Contains(string(body), "<Marker></Marker>") || !strings.Contains(string(body), "<Name>another</Name>") {
		t.Fatalf("last page: %d %s", code, body)
	}
	code, body = sendBody(t, h, "GET", "/projects/p/schemas?type=EXTERNAL", nil, nil)
	if code != 200 || strings.Contains(string(body), "<Schema>") {
		t.Fatalf("type filter: %d %s", code, body)
	}
	for _, path := range []string{"/projects/p/schemas/missing", "/projects/missing/schemas/alpha"} {
		code, body = sendBody(t, h, "GET", path, nil, nil)
		if code != 404 {
			t.Fatalf("missing: %d %s", code, body)
		}
	}
	if _, err := s.Engine.Execute(context.Background(), "p", "alpha", "create table t(id bigint)"); err != nil {
		t.Fatal(err)
	}
	code, body = sendBody(t, h, "GET", "/projects/p/schemas/ALPHA/tables/t", nil, nil)
	if code != 200 || !strings.Contains(string(body), "<SchemaName>alpha</SchemaName>") {
		t.Fatalf("uppercase schema object path: %d %s", code, body)
	}
	code, body = sendBody(t, h, "DELETE", "/projects/p/schemas/ALPHA", nil, nil)
	if code != 204 {
		t.Fatalf("cascade: %d %s", code, body)
	}
	if _, err := s.Engine.Table(context.Background(), "p", "alpha", "t"); err == nil {
		t.Fatal("cascade left table metadata")
	}
	code, body = sendBody(t, h, "DELETE", "/projects/p/schemas/alpha", nil, nil)
	if code != 204 {
		t.Fatalf("delete: %d %s", code, body)
	}
	code, body = sendBody(t, h, "GET", "/projects/p/schemas/alpha", nil, nil)
	if code != 404 || !strings.Contains(string(body), "NoSuchObject") {
		t.Fatalf("deleted schema: %d %s", code, body)
	}
	for _, path := range []string{"/projects/p/schemas/alpha/tables", "/projects/p/schemas/alpha/resources", "/projects/p/schemas/alpha/registration/functions"} {
		code, body = sendBody(t, h, "GET", path, nil, nil)
		if code != 404 || !strings.Contains(string(body), "NoSuchObject") {
			t.Fatalf("deleted schema object route: %d %s", code, body)
		}
	}
	code, body = sendBody(t, h, "POST", "/projects/p/instances?curr_schema=alpha", []byte("<Instance/>"), nil)
	if code != 404 || !strings.Contains(string(body), "NoSuchObject") {
		t.Fatalf("old connection recreated deleted schema: %d %s", code, body)
	}
	code, body = sendBody(t, h, "POST", "/projects/p/schemas", []byte("<Schema><Name>bad"), nil)
	if code != 400 || !strings.Contains(string(body), "SchemaParseError") {
		t.Fatalf("invalid XML: %d %s", code, body)
	}
}
