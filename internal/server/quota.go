package server

import (
	"context"
	"net/http"
	"net/url"
	"strings"
)

const quotaRoutePrefix = "/__quota/"

type quotaRouteKey struct{}

// A path prefix supplies a distinct Tunnel endpoint for each quota on one
// listener. It belongs to the endpoint, outside the signed resource path.
func splitQuotaRoute(r *http.Request) *http.Request {
	rest, ok := strings.CutPrefix(r.URL.EscapedPath(), quotaRoutePrefix)
	if !ok {
		return r
	}
	escapedQuota, path, ok := strings.Cut(rest, "/")
	if !ok || escapedQuota == "" {
		return r
	}
	quota, err := url.PathUnescape(escapedQuota)
	if err != nil {
		return r
	}
	escapedPath := "/" + path
	resourcePath, err := url.PathUnescape(escapedPath)
	if err != nil {
		return r
	}
	r = r.WithContext(context.WithValue(r.Context(), quotaRouteKey{}, quota))
	endpoint := *r.URL
	endpoint.Path = resourcePath
	endpoint.RawPath = escapedPath
	r.URL = &endpoint
	return r
}

func quotaEndpoint(host, quota string) string {
	if quota != "default" {
		return host + quotaRoutePrefix + url.PathEscape(quota)
	}
	return host
}

func requestQuota(r *http.Request, discovery bool) (name string, conflict bool) {
	name, _ = r.Context().Value(quotaRouteKey{}).(string)
	params := r.URL.Query()
	keys := []string{"quotaName", "quota_name"}
	if discovery {
		keys = append(keys, "quota")
	}
	for _, key := range keys {
		for _, value := range params[key] {
			if value != "" {
				if name != "" && value != name {
					return name, true
				}
				name = value
			}
		}
	}
	return name, false
}
