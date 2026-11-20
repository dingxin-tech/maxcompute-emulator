package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSTSFixtureRotatesAndInheritsACLs(t *testing.T) {
	owner := Credential{Secret: "owner-fixture", Read: []string{"p.*"}}
	other := Credential{Secret: "other-fixture", Read: []string{"p.*"}}
	s, h := fixture(t, Config{
		TestMode: true, AuthMode: "strict",
		Credentials: map[string]Credential{"owner": owner, "other": other},
	})
	call := func(method, path, key string, c Credential, want int) []byte {
		t.Helper()
		req, err := http.NewRequest(method, h.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if key != "" {
			req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
			req.Header.Set("Authorization", "ODPS "+key+":"+authSignature(req, c.Secret, key))
			req.Header.Set("authorization-sts-token", c.Token)
		}
		res, err := h.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("%s returned HTTP %d, want %d", method, res.StatusCode, want)
		}
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	call("POST", stsFixturePath, "", Credential{}, 401)
	call("POST", stsFixturePath+"?ttl_seconds=0", "owner", owner, 400)
	var descriptor struct{ Path string }
	if err := json.Unmarshal(call("POST", stsFixturePath+"?ttl_seconds=60", "owner", owner, 201), &descriptor); err != nil {
		t.Fatal(err)
	}
	get := func() (string, Credential) {
		t.Helper()
		var reply struct {
			AccessKeyID, AccessKeySecret, SecurityToken string
			Expiration                                  time.Time
		}
		if err := json.Unmarshal(call("GET", descriptor.Path, "", Credential{}, 200), &reply); err != nil {
			t.Fatal(err)
		}
		if reply.AccessKeyID == "" || reply.AccessKeySecret == "" || reply.SecurityToken == "" ||
			reply.AccessKeySecret == owner.Secret || time.Until(reply.Expiration) <= 0 {
			t.Fatal("invalid temporary credential envelope")
		}
		return reply.AccessKeyID, Credential{Secret: reply.AccessKeySecret, Token: reply.SecurityToken}
	}
	oldID, old := get()
	call("GET", "/api/projects/p", oldID, old, 200)
	call("POST", "/api/projects/p/resources", oldID, old, 403)
	call("POST", descriptor.Path, "other", other, 404)
	call("POST", descriptor.Path, oldID, old, 403)

	call("POST", descriptor.Path, "owner", owner, 200)
	newID, fresh := get()
	if newID == oldID || fresh.Token == old.Token || fresh.Secret == old.Secret {
		t.Fatal("rotation kept a previous credential field")
	}
	call("GET", "/api/projects/p", oldID, old, 401)
	call("GET", "/api/projects/p", newID, Credential{Secret: fresh.Secret, Token: old.Token}, 401)
	call("GET", "/api/projects/p", newID, fresh, 200)

	key := strings.TrimPrefix(descriptor.Path, stsFixturePath+"/")
	s.mu.Lock()
	expired := s.stsFixtures[key]
	expired.credential.ExpiresAt = time.Now().Add(-time.Second)
	s.stsFixtures[key] = expired
	s.mu.Unlock()
	call("GET", descriptor.Path, "", Credential{}, 404)
	call("GET", "/api/projects/p", newID, fresh, 401)
	call("DELETE", descriptor.Path, "owner", owner, 204)
	call("GET", descriptor.Path, "", Credential{}, 404)
}

func TestSTSFixtureRequiresStrictTestMode(t *testing.T) {
	for _, cfg := range []Config{{AuthMode: "strict"}, {TestMode: true}} {
		_, h := fixture(t, cfg)
		status, _, _ := request(t, h, "POST", stsFixturePath, "")
		if status != http.StatusNotFound {
			t.Fatalf("fixture outside strict test mode returned HTTP %d", status)
		}
	}
}
