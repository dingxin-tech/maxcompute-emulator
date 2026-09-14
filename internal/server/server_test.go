package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/dingxin-tech/maxcompute-emulator/internal/engine"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

func fixture(t *testing.T, c Config) (*Server, *httptest.Server) {
	t.Helper()
	e, err := engine.Open("", 10000)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	values := make([]string, 2048)
	for i := range values {
		values[i] = fmt.Sprintf("(%d,'%s')", i, strings.Repeat("value", 20))
	}
	_, err = e.Execute(context.Background(), "p", "default", "create table t(id bigint,s string);insert into t values "+strings.Join(values, ","))
	if err != nil {
		t.Fatal(err)
	}
	s := New(e, c)
	h := httptest.NewServer(s)
	t.Cleanup(h.Close)
	return s, h
}
func request(t *testing.T, h *httptest.Server, method, path, encoding string) (int, http.Header, []byte) {
	t.Helper()
	r, _ := http.NewRequest(method, h.URL+path, nil)
	if encoding == "" {
		encoding = "identity"
	}
	if encoding != "" {
		r.Header.Set("Accept-Encoding", encoding)
	}
	resp, e := h.Client().Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(resp.Body)
	if e != nil {
		t.Fatal(e)
	}
	return resp.StatusCode, resp.Header, b
}
func sessionID(t *testing.T, h *httptest.Server) string {
	t.Helper()
	code, _, b := request(t, h, "POST", "/projects/p/tables/t?downloads&asyncmode=true&quotaName=q", "")
	if code != 200 {
		t.Fatalf("%d %s", code, b)
	}
	var m map[string]any
	if e := json.Unmarshal(b, &m); e != nil {
		t.Fatal(e)
	}
	if m["RecordCount"] != float64(2048) || m["Status"] != "normal" || m["QuotaName"] != "q" || m["Owner"] == nil || m["Initiated"] == nil {
		t.Fatal(m)
	}
	return m["DownloadID"].(string)
}
func unchunk(t *testing.T, b []byte) []byte {
	t.Helper()
	if len(b) < 8 {
		t.Fatal("short Arrow frame")
	}
	size := int(binary.BigEndian.Uint32(b))
	b = b[4:]
	if size != 65536 {
		t.Fatal(size)
	}
	out := []byte{}
	tab := crc32.MakeTable(crc32.Castagnoli)
	for len(b) > size+4 {
		chunk := b[:size]
		sum := binary.BigEndian.Uint32(b[size:])
		if crc32.Checksum(chunk, tab) != sum {
			t.Fatal("chunk checksum")
		}
		out = append(out, chunk...)
		b = b[size+4:]
	}
	out = append(out, b[:len(b)-4]...)
	if crc32.Checksum(out, tab) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		t.Fatal("global checksum")
	}
	mr := ipc.NewMessageReader(bytes.NewReader(out))
	defer mr.Release()
	msg, e := mr.Message()
	if e != nil {
		t.Fatal(e)
	}
	if msg.Type() != ipc.MessageRecordBatch {
		t.Fatalf("expected schema-less batch, got %s", msg.Type())
	}
	if _, e = mr.Message(); !errors.Is(e, io.EOF) {
		t.Fatalf("one batch required: %v", e)
	}
	return out
}
func TestArrowCompressionChunkCRCAndRawLimit(t *testing.T) {
	_, h := fixture(t, Config{})
	id := sessionID(t, h)
	path := "/projects/p/tables/t?downloadid=" + id + "&data&arrow&rowrange=(0,2048)"
	code, header, raw := request(t, h, "GET", path, "")
	if code != 200 {
		t.Fatalf("%d %s", code, raw)
	}
	if len(raw) < 65536 {
		t.Fatal("fixture must span chunks")
	}
	unchunk(t, raw)
	for _, alg := range []string{"zstd", "lz4_frame"} {
		code, head, b := request(t, h, "GET", path, alg)
		if code != 200 || head.Get("Content-Encoding") != alg {
			t.Fatalf("%d %v", code, head)
		}
		var got []byte
		var err error
		if alg == "zstd" {
			r, e := zstd.NewReader(bytes.NewReader(b))
			if e != nil {
				t.Fatal(e)
			}
			got, err = io.ReadAll(r)
			r.Close()
		} else {
			got, err = io.ReadAll(lz4.NewReader(bytes.NewReader(b)))
		}
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, got) {
			t.Fatalf("%s changes payload", alg)
		}
	}
	code, head, small := request(t, h, "GET", path+"&raw_size=2000", "")
	if code != 200 || len(small) >= len(raw) || head.Get("Last-Modified") != header.Get("Last-Modified") {
		t.Fatalf("%d size=%d", code, len(small))
	}
	unchunk(t, small)
	code, _, b := request(t, h, "GET", path, "deflate")
	if code != 400 || !bytes.Contains(b, []byte("InvalidCompression")) {
		t.Fatalf("%d %s", code, b)
	}
}
func TestSessionBindingErrorsAndCompletion(t *testing.T) {
	_, h := fixture(t, Config{MaxSessions: 1})
	id := sessionID(t, h)
	base := "/projects/p/tables/t?downloadid=" + id
	for _, path := range []string{"/projects/other/tables/t?downloadid=" + id, base + "&curr_schema=other", base + "&partition=ds=other"} {
		code, _, _ := request(t, h, "GET", path, "")
		if code != 404 {
			t.Fatal(path, code)
		}
	}
	for _, suffix := range []string{"&rowrange=(-1,2)", "&rowrange=(99999,1)", "&rowrange=(0,9999999999999999999999999)", "&rowrange=(0,2)&columns=missing", "&rowrange=(0,2)&columns=id,id", "&rowrange=(0,2)&raw_size=-1"} {
		code, _, b := request(t, h, "GET", base+"&data"+suffix, "")
		if code != 400 {
			t.Fatalf("%s: %d %s", suffix, code, b)
		}
	}
	code, _, _ := request(t, h, "POST", "/projects/p/tables/t?downloads", "")
	if code != 429 {
		t.Fatal(code)
	}
	code, _, _ = request(t, h, "POST", base, "")
	if code != 200 {
		t.Fatal(code)
	}
	code, _, _ = request(t, h, "GET", base, "")
	if code != 404 {
		t.Fatal(code)
	}
	sessionID(t, h)
}
func TestSessionExpiry(t *testing.T) {
	s, h := fixture(t, Config{})
	id := sessionID(t, h)
	s.mu.Lock()
	s.sessions[id].Created = time.Now().Add(-time.Hour)
	s.mu.Unlock()
	code, _, _ := request(t, h, "GET", "/projects/p/tables/t?downloadid="+id, "")
	if code != 404 {
		t.Fatal(code)
	}
}
