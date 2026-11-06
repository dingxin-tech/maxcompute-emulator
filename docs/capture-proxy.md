# Capture SDK requests with a fixed upstream proxy

Build the candidate image from source. Start the proxy with the base URL you normally give the SDK.
For an endpoint such as `https://maxcompute.example.test/api`, keep `/api` in `--upstream` and
give the SDK the local URL `http://127.0.0.1:8081`.

```sh
docker build --platform linux/amd64 -t maxcompute-emulator:proxy .
docker run --rm --name mc-proxy --platform linux/amd64 \
  -p 127.0.0.1:8081:8080 maxcompute-emulator:proxy \
  proxy --listen 0.0.0.0:8080 --upstream https://maxcompute.example.test/api \
  --capture-headers User-Agent,traceparent,tracestate > requests.jsonl
```

Use the SDK's existing test identity and project. A request such as
`POST /projects/test_project/instances` reaches the configured upstream with its original method,
query, body and end-to-end headers. The proxy uses the upstream host and prepends the configured
base path. HTTPS upstreams use the system trust store. The SDK-to-proxy connection in this
example uses local HTTP.

Stdout contains one JSON object per completed request. Header names follow HTTP canonical casing.
Repeated values are arrays and absent headers have no entry. The path excludes the query string.
The upstream status and Request ID let tests match a capture to the service response.

```json
{"time":"2026-01-01T00:00:00Z","method":"POST","path":"/projects/test_project/instances","request_headers":{"Tracestate":["test=example"]},"status":201,"request_id":"example-request-id"}
```

Choose headers with `--capture-headers` when their values are suitable for test evidence.
The default capture contains only request and response metadata. Credential-related headers,
such as authorization, authentication, token and cookie headers, are rejected as capture choices.
Bodies and query values stay out of the capture. An upstream transport failure produces HTTP 502
with `"error":"upstream_transport"` in the record.

Configure the SDK endpoint for this mode. A separate Tunnel endpoint or an absolute redirect can
lead the client to another host. To observe a separate endpoint, run a second proxy instance for it.
