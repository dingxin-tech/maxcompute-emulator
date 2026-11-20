package server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const stsFixturePath = "/__test/sts"
const maxSTSFixtures = 64

type stsFixture struct {
	owner, accessID string
	credential      Credential
}

// credential looks up the configured principals and the current generation of
// each temporary test credential. A rotation removes the previous generation.
func (s *Server) credential(accessID string) (Credential, bool) {
	if c, ok := s.cfg.Credentials[accessID]; ok {
		return c, true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, fixture := range s.stsFixtures {
		if fixture.accessID == accessID {
			return fixture.credential, true
		}
	}
	return Credential{}, false
}

func (s *Server) stsFixtureAPI(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.TestMode || s.cfg.AuthMode != "strict" {
		http.NotFound(w, r)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, stsFixturePath)
	if key != "" {
		key = strings.TrimPrefix(key, "/")
		if len(key) != 32 || strings.Contains(key, "/") {
			http.NotFound(w, r)
			return
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodGet {
		s.mu.Lock()
		fixture, ok := s.stsFixtures[key]
		s.mu.Unlock()
		if !ok || !time.Now().Before(fixture.credential.ExpiresAt) {
			http.NotFound(w, r)
			return
		}
		jsonResponse(w, http.StatusOK, map[string]string{
			"AccessKeyId":     fixture.accessID,
			"AccessKeySecret": fixture.credential.Secret,
			"SecurityToken":   fixture.credential.Token,
			"Expiration":      fixture.credential.ExpiresAt.UTC().Format(time.RFC3339),
		})
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		fail(w, r, http.StatusMethodNotAllowed, "InvalidMethod", fmt.Errorf("GET, POST or DELETE required"))
		return
	}
	if !s.authenticate(w, r) {
		return
	}
	owner := trace(r).authenticatedKey
	principal, ok := s.cfg.Credentials[owner]
	if !ok {
		fail(w, r, http.StatusForbidden, "NoPermission", fmt.Errorf("a configured fixture principal is required"))
		return
	}
	ttl, err := fixtureTTL(r.URL.Query().Get("ttl_seconds"))
	if err != nil {
		fail(w, r, http.StatusBadRequest, "InvalidParameter", err)
		return
	}
	now := time.Now().UTC().Truncate(time.Second)
	s.mu.Lock()
	if key != "" {
		fixture, exists := s.stsFixtures[key]
		if !exists || fixture.owner != owner {
			s.mu.Unlock()
			http.NotFound(w, r)
			return
		}
	} else if r.Method == http.MethodDelete {
		s.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodDelete {
		delete(s.stsFixtures, key)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	status := http.StatusOK
	if key == "" {
		for id, fixture := range s.stsFixtures {
			if now.After(fixture.credential.ExpiresAt.Add(5 * time.Minute)) {
				delete(s.stsFixtures, id)
			}
		}
		if len(s.stsFixtures) >= maxSTSFixtures {
			s.mu.Unlock()
			fail(w, r, http.StatusTooManyRequests, "ResourceLimit", fmt.Errorf("temporary credential fixture limit"))
			return
		}
		key = id()
		status = http.StatusCreated
	}
	s.stsFixtures[key] = stsFixture{
		owner: owner, accessID: "fixture-sts-" + id(),
		credential: Credential{
			Secret: id(), Token: id(), ExpiresAt: now.Add(ttl),
			Read: principal.Read, Write: principal.Write,
		},
	}
	s.mu.Unlock()
	jsonResponse(w, status, map[string]string{"path": stsFixturePath + "/" + key})
}

func fixtureTTL(value string) (time.Duration, error) {
	if value == "" {
		return time.Minute, nil
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds < 1 || seconds > 300 {
		return 0, fmt.Errorf("ttl_seconds must be between 1 and 300")
	}
	return time.Duration(seconds) * time.Second, nil
}
