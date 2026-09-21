package server

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Credentials are local test fixtures, never production credentials. Empty ACLs deny.
type Credential struct {
	Secret    string    `json:"secret"`
	Token     string    `json:"token,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	Read      []string  `json:"read"`
	Write     []string  `json:"write,omitempty"`
}

func (s *Server) authDescription() string {
	if s.cfg.AuthMode == "strict" {
		return "strict; local ODPS v2/v4 signature, date, STS and ACL fixtures"
	}
	return "test-only; signatures not validated"
}
func canonicalRequest(r *http.Request) string {
	headers := map[string]string{"content-md5": "", "content-type": ""}
	for k, v := range r.Header {
		k = strings.ToLower(k)
		if k == "content-md5" || k == "content-type" || k == "date" || strings.HasPrefix(k, "x-odps-") {
			headers[k] = strings.Join(v, ",")
		}
	}
	for k, v := range r.URL.Query() {
		if strings.HasPrefix(k, "x-odps-") {
			headers[k] = v[0]
		}
	}
	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(r.Method + "\n")
	for _, k := range keys {
		if strings.HasPrefix(k, "x-odps-") {
			b.WriteString(k + ":")
		}
		b.WriteString(headers[k] + "\n")
	}
	// REST SDK excludes the endpoint /api prefix; Storage signs /api/storage/vN.
	resource := r.URL.Path
	if strings.HasPrefix(resource, "/api/projects/") {
		resource = strings.TrimPrefix(resource, "/api")
	}
	b.WriteString(resource)
	q := r.URL.Query()
	keys = keys[:0]
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		if i == 0 {
			b.WriteByte('?')
		} else {
			b.WriteByte('&')
		}
		b.WriteString(k)
		if v := q.Get(k); v != "" {
			b.WriteString("=" + v)
		}
	}
	return b.String()
}
func authSignature(r *http.Request, secret, credential string) string {
	key := []byte(secret)
	parts := strings.Split(credential, "/")
	if len(parts) == 5 {
		key = []byte("aliyun_v4" + secret)
		for _, v := range parts[1:] {
			h := hmac.New(sha256.New, key)
			h.Write([]byte(v))
			key = h.Sum(nil)
		}
	}
	h := hmac.New(sha1.New, key)
	h.Write([]byte(canonicalRequest(r)))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) bool {
	if s.cfg.AuthMode == "" || s.cfg.AuthMode == "permissive" {
		return true
	}
	deny := func(status int, code string) bool {
		fail(w, r, status, code, fmt.Errorf("request authentication or permission check failed"))
		return false
	}
	if s.cfg.AuthMode != "strict" {
		return deny(401, "Unauthorized")
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "ODPS ") {
		return deny(401, "Unauthorized")
	}
	cred, sig, ok := strings.Cut(strings.TrimPrefix(auth, "ODPS "), ":")
	if !ok {
		return deny(401, "Unauthorized")
	}
	parts := strings.Split(cred, "/")
	if len(parts) != 1 && (len(parts) != 5 || parts[3] != "odps" || parts[4] != "aliyun_v4_request" || parts[2] == "") {
		return deny(401, "Unauthorized")
	}
	c, ok := s.cfg.Credentials[parts[0]]
	if !ok || c.Secret == "" {
		return deny(401, "Unauthorized")
	}
	date, err := http.ParseTime(r.Header.Get("Date"))
	if err != nil || time.Since(date) > 15*time.Minute || time.Until(date) > 15*time.Minute || !c.ExpiresAt.IsZero() && time.Now().After(c.ExpiresAt) {
		return deny(401, "Unauthorized")
	}
	if len(parts) == 5 && parts[1] != date.UTC().Format("20060102") {
		return deny(401, "Unauthorized")
	}
	if !hmac.Equal([]byte(c.Token), []byte(r.Header.Get("authorization-sts-token"))) || !hmac.Equal([]byte(sig), []byte(authSignature(r, c.Secret, cred))) {
		return deny(401, "Unauthorized")
	}
	t := trace(r)
	if strings.TrimPrefix(r.URL.Path, "/api") == "/init" && r.Method == "POST" {
		ok := false
		for _, g := range c.Write {
			if g == "*" || g == s.cfg.Project+".*" {
				ok = true
			}
		}
		if !ok {
			return deny(403, "NoPermission")
		}
	}
	if t != nil && t.Project != "" {
		grants := c.Read
		storageRead := false
		path := strings.TrimPrefix(r.URL.Path, "/api")
		if path == "/storage/v2" || path == "/storage/v3" {
			switch r.URL.Query().Get("Action") {
			case "TableCreateReadSession", "TableGetReadSession", "TableRead", "InstanceCreateReadSession", "InstanceGetReadSession", "InstanceRead":
				storageRead = true
			}
		}

		// Only a Tunnel download create/complete establishes a read session, so
		// it stays on read grants. A metadata-plane POST named "create" is a
		// write: resources and functions are project objects, not read sessions.
		readEstablishing := (t.Action == "create" || t.Action == "complete") && t.Plane != "rest"
		if !storageRead && r.Method != "GET" && r.Method != "HEAD" && !readEstablishing {
			grants = c.Write
		}
		allowed := false
		for _, g := range grants {
			if g == "*" || g == t.Project+".*" || t.Table != "" && g == t.Project+"."+t.Table {
				allowed = true
			}
		}
		if !allowed {
			return deny(403, "NoPermission")
		}
	}
	return true
}
