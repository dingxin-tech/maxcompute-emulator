package server

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
)

func TestMetadataPaginationAndIsolation(t *testing.T) {
	e, err := engine.Open("", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_, err = e.Execute(context.Background(), "p", "default", "create table aa(id bigint) partitioned by(ds string);create table ab(id bigint);create table zz(id bigint);alter table aa add partition(ds='a');alter table aa add partition(ds='b')")
	if err != nil {
		t.Fatal(err)
	}
	s := New(e, Config{})
	for _, tc := range []struct {
		path, want, absent string
		status             int
	}{
		{"/projects/p/tables?name=a&maxitems=1", "<Name>aa</Name>", "<Name>ab</Name>", 200},
		{"/projects/p/tables?name=a&maxitems=1&marker=aa", "<Name>ab</Name>", "<Name>zz</Name>", 200},
		{"/projects/p/tables/aa?partitions&maxitems=1", "Value=\"a\"", "Value=\"b\"", 200},
		{"/projects/p/tables/aa?partition=ds%3D%27missing%27", "NoSuchPartition", "<Schema>", 404},
		{"/projects/other/tables", "NoSuchProject", "<Name>aa</Name>", 404},
	} {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.want) || strings.Contains(w.Body.String(), tc.absent) {
			t.Fatalf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
	}
}
