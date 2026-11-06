// Package captureproxy forwards HTTP requests to one endpoint and records selected headers.
package captureproxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

type entry struct {
	Time           time.Time           `json:"time"`
	Method         string              `json:"method"`
	Path           string              `json:"path"`
	RequestHeaders map[string][]string `json:"request_headers,omitempty"`
	Status         int                 `json:"status"`
	RequestID      string              `json:"request_id,omitempty"`
	Error          string              `json:"error,omitempty"`
}

type entryKey struct{}

var headerName = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")

// New creates a reverse proxy to a fixed upstream. The capture contains only the
// selected request header values, the path without its query, and response metadata.
func New(upstream string, captureHeaders []string, output io.Writer) (http.Handler, error) {
	target, err := url.Parse(upstream)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") ||
		target.Hostname() == "" || target.User != nil || target.ForceQuery ||
		target.RawQuery != "" || target.Fragment != "" {
		return nil, errors.New("upstream must be an HTTP(S) endpoint without credentials, query, or fragment")
	}
	var names []string
	for _, name := range captureHeaders {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if !headerName.MatchString(name) || sensitive(name) {
			return nil, errors.New("capture header must be a valid name and cannot be a credential header")
		}
		names = append(names, http.CanonicalHeaderKey(name))
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	reverse := &httputil.ReverseProxy{
		Transport: transport,
		ErrorLog:  log.New(io.Discard, "", 0),
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target)
			// Keep the exact query used to sign the request, including order and escapes.
			request.Out.URL.RawQuery = request.In.URL.RawQuery
		},
		ModifyResponse: func(response *http.Response) error {
			captured := response.Request.Context().Value(entryKey{}).(*entry)
			captured.Status = response.StatusCode
			captured.RequestID = response.Header.Get("X-Odps-Request-Id")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, _ error) {
			captured := r.Context().Value(entryKey{}).(*entry)
			captured.Status = http.StatusBadGateway
			captured.Error = "upstream_transport"
			http.Error(w, http.StatusText(captured.Status), captured.Status)
		},
	}
	encoder := json.NewEncoder(output)
	var logMu sync.Mutex
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured := &entry{Time: time.Now().UTC(), Method: r.Method, Path: r.URL.EscapedPath()}
		defer func() {
			logMu.Lock()
			defer logMu.Unlock()
			if err := encoder.Encode(captured); err != nil {
				log.Print("proxy capture write failed")
			}
		}()
		if r.Method == http.MethodConnect || r.Header.Get("Upgrade") != "" {
			captured.Status = http.StatusMethodNotAllowed
			captured.Error = "tunnel_unsupported"
			http.Error(w, http.StatusText(captured.Status), captured.Status)
			return
		}
		if r.URL.IsAbs() {
			captured.Status = http.StatusBadRequest
			captured.Error = "absolute_target"
			http.Error(w, http.StatusText(captured.Status), captured.Status)
			return
		}
		if len(names) > 0 {
			captured.RequestHeaders = make(map[string][]string, len(names))
			for _, name := range names {
				if name == "Host" {
					captured.RequestHeaders[name] = []string{r.Host}
				} else if values := r.Header.Values(name); len(values) > 0 {
					captured.RequestHeaders[name] = values
				}
			}
		}
		reverse.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), entryKey{}, captured)))
	}), nil
}

func sensitive(name string) bool {
	name = strings.NewReplacer("-", "", "_", "").Replace(strings.ToLower(name))
	for _, term := range []string{"authorization", "authentication", "cookie", "token",
		"secret", "password", "credential", "signature", "accesskey", "accessid", "apikey"} {
		if strings.Contains(name, term) {
			return true
		}
	}
	return false
}
