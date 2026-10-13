# Reliability test controls

The default service remains permissive. Fault injection is off. These controls
are synthetic local/CI fixtures, not production IAM or quota enforcement.

## JSON session logs

JSON is the default log format; use `--log-format text` for interactive output.
Each Tunnel download request records `action` (`create`, `reload`, `read`,
`complete`), `download_id_hash`, `project`, `table`, `partition_present`, `start`,
`count`, `columns_count`, `format`, `compression`, HTTP `status`, `error_code`,
`elapsed_ms`, `quota` and `request_id`. `columns_count=0` means all columns were
requested. Ranges describe the request, including fault-injected responses.

Each resources or functions request logs a separate `rest` event with `action`
(`create`, `read`, `update`, `delete` — the request verb), `object`
(`resources` or `functions`), `project`, `status`, `error_code` and
`elapsed_ms`. Object names are not logged, so a resource name never reaches the
log stream.
The timestamp is emitted when the handler finishes. Hashes are stable only
within one process; logs omit raw session IDs, query strings and credentials.

## Protocol faults

For Docker, explicitly opt into a test network and bind the host port locally:

```sh
docker run --rm -p 127.0.0.1:8080:8080 maxcompute/maxcompute-emulator:1.2.0 \
  --listen 0.0.0.0:8080 --test-mode --test-network --quotas named
```

Without
`--test-network`, test mode requires a numeric loopback listen address.
`/__test` returns 404 unless test mode is enabled. These management endpoints
are unauthenticated and must be confined to your test network.

```sh
curl -X PUT localhost:8080/__test/faults/retry -H 'Content-Type: application/json' -d '{
  "match":{"action":"read","project":"test_project","table":"e2e_types","format":"arrow","attempt":1},
  "effect":{"type":"http_error","status":429,"code":"FlowExceeded"},
  "times":1,"ttl_seconds":60
}'
curl localhost:8080/__test/faults/retry
curl -X DELETE localhost:8080/__test/faults/retry
curl -X DELETE localhost:8080/__test/faults

curl -X PUT localhost:8080/__test/faults/udf-retry -H 'Content-Type: application/json' -d '{
  "match":{"plane":"rest","object":"resources","action":"create","project":"test_project"},
  "effect":{"type":"http_error","status":500},
  "times":1,"ttl_seconds":60
}'
```

`plane` defaults to the Tunnel download plane. `"plane":"rest"` selects the
metadata plane (resources and registered functions), where `object` is required
and `action` is the request verb; only `http_error` and `delay` are available
there, because that plane has no stream, session or format to corrupt. A
metadata fault rejects the request before the handler runs, so it simulates a
transient server failure without leaving a partially created object — which is
what a client retry loop needs to be tested against. Tunnel rules and metadata
rules never match each other's requests, and `table`, `format` and `quota` are
Tunnel-only match dimensions.

Tunnel rules match an explicit download action, with optional project, table,
format and effective quota. `attempt` is the first matching request ordinal eligible
for injection (0 means immediately); `times` is the hit budget. Ordinals reset
on PUT. Rules are checked in lexicographic ID order; at most one applies per
request. GET returns attempts, hits and expiration. Up to 128 rules can exist;
TTL defaults to 60 seconds and cannot exceed 3600. PUT replaces the rule.

| Effect | Parameters and behavior |
| --- | --- |
| `http_error` | `status` 400–599, optional `code`; 429 defaults to FlowExceeded and Retry-After: 1 |
| `disconnect_after_bytes` | `bytes`: incomplete HTTP body after this many encoded bytes |
| `disconnect_after_rows` | `rows`: emit this many rows/batches, then incomplete HTTP body |
| `early_eof` | `rows`: valid stream ending before the requested count |
| `crc_mismatch` | Change the stream checksum |
| `malformed_protobuf` | Invalid protobuf wire type; match format protobuf |
| `malformed_arrow` | Invalid IPC metadata with valid outer CRC; match format arrow |
| `empty_arrow_batch` | Actual zero-row IPC batch, not an empty table response |
| `oversized_arrow_batch` | Add a duplicate row beyond requested range (nonempty ranges) |
| `delay` | `delay_ms` up to 60000; cancellation interrupts waiting |
| `expire_session` | Remove the selected download session and return 404 NoSuchDownload |
| `complete_error` | Fail finalization without removing the session; default 500 InternalError |

Stream effects require action read. Arrow effects require format arrow. Row
truncation applies before encoding; Arrow uses complete IPC batches at the
truncation boundary. Resume from rows actually consumed; do not replay committed
rows. An injected failure does not prove that a consumer retries correctly:
assert its returned values, retry budget, cancellation and finalization counts.
`delay` affects the HTTP request handler, not TCP connection establishment.

## Quotas

`default` always exists. `--quotas named,other` configures additional names.
Missing named quotas return 404 `QuotaNotExist`. Router replies retain their
plain endpoint body, adding `x-odps-tunnel-quota-name`. Table download create and
reload return `QuotaName`; a session retains its original effective quota.
A fault match may include `"quota":"named"` for a bounded 429/FlowExceeded test.
This does not model production throughput scheduling or regional quota setup.

## Optional strict authentication

Mount a JSON file of **invented test credentials** and select strict mode:

```json
{
  "local-ak": {
    "secret": "local-test-secret",
    "token": "optional-test-sts-token",
    "expires_at": "2099-01-01T00:00:00Z",
    "read": ["test_project.*"],
    "write": ["test_project.*"]
  }
}
```

```sh
docker run --rm -p 127.0.0.1:8080:8080 \
  -v "$PWD/auth.json:/tmp/auth.json:ro" maxcompute/maxcompute-emulator:1.2.0 \
  --listen 0.0.0.0:8080 --auth-mode strict --auth-config /tmp/auth.json
```

Strict mode checks ODPS v2/v4 signatures using the SDK canonical request,
Date within ±15 minutes, credential expiry and exact STS token. Invalid/missing
credentials return 401 `Unauthorized`; insufficient grants return 403
`NoPermission`. Grants are `project.table`, `project.*` or `*`; empty lists deny.
GET, download create/complete and Storage read actions use read grants. SQL
submission and other writes require write grants. On the metadata plane the verb
decides, not the name: creating a resource or registering a function is a write
even though the Tunnel plane reserves `create` for read sessions, so those
requests need a write grant, while listing and reading metadata needs a read
grant. Project-wide metadata access requires a project-wide grant. Health probes remain unauthenticated.
This is a deterministic ACL fixture, not RAM policy evaluation or real STS
validation. Keep real MaxCompute authentication and quota gates.

## Type fixtures

[examples/types.sql](../examples/types.sql), bundled as `/opt/emulator/types.sql`
(use `--seed /opt/emulator/types.sql`), uses public SQL to create nanosecond
TIMESTAMP_NTZ, pre-1970 DATE/DATETIME, signed BIGINT limits, decimal rounding,
NULL/empty arrays/maps, nested structs/maps and independent MAP_KEYS/MAP_VALUES
projection tables. MAP accepts alternating keys/values; the earlier two-ARRAY
extension also remains available. Decimal/integer overflow fails atomically.
ODPS has no unsigned BIGINT; unsigned boundary fixtures use DECIMAL(20,0).
The Java SDK rejects Long.MIN_VALUE while the SQL engine can store it; use
Long.MIN_VALUE+1 for a successful Java row-reader fixture and retain that client
boundary as a negative test. Do not infer wire compatibility from SQL alone.

## Verification

Run Go tests in the Docker build image and Java tests against the candidate
image as described in [build.md](build.md). `ReliabilityTest` starts its own
strict-mode Testcontainer. The C++ wire probe accepts `--faults` against a
separate test-mode container; it verifies that corrupted Arrow streams fail
and the subsequent normal request succeeds. It is not the complete SDK HTTP
client or ClickHouse E2E runner.

## Write-stage faults (1.2.0)

Use version 1.2.0 or a reviewed source build for these controls. Enable `--test-mode --test-network` with a loopback host port as
above. Select an explicit plane, object, action and UUID table so unrelated reads
and metadata requests cannot consume the fault.

| Plane | Object | Actions |
| --- | --- | --- |
| `tunnel` | `upload`, `upsert`, `stream` | `create`, `reload`, `write`, `commit`, `abort` |
| `storage` | `write` | `create_session`, `create_stream`, `write`, `close_stream`, `commit` |

Only requests actually sent by the SDK can trigger a stage. For example, Storage
realtime may create streams and write without batch close/commit; a consumer test
must not claim those stages merely because the rule was installed.

```sh
curl -X PUT http://127.0.0.1:8080/__test/faults/write-ack \
  -H 'Content-Type: application/json' -d '{
  "match":{"plane":"storage","object":"write","action":"write",
           "project":"test_project","table":"case_uuid","attempt":3},
  "effect":{"type":"lost_ack"},"times":1,"ttl_seconds":60
}'
curl http://127.0.0.1:8080/__test/faults/write-ack
```

Write rules accept `http_error` (explicit status 400–599), `delay` and `lost_ack`.
HTTP errors occur before mutation. Lost ACK executes the ordinary mutation, then
closes the connection before the success response reaches the client. Retrying uses
the normal session/block/pack identity; injection adds no deduplication behavior.
An `attempt` ordinal counts matching requests, not committed checkpoints. Assert
that every planned rule has hits and independently read back the resulting rows.

`GET /__test/write-metrics` returns `active`, `peak`, `active_data` and `peak_data`.
Use data counters for pressure evidence; concurrent session creation alone is not
concurrent data upload. `POST /__test/write-metrics` resets counters only while idle.
Assert active counters return to zero after cancellation/cleanup. This measures
local request overlap, not production throughput or service durability.

## Concurrent admission capacity

`--max-concurrent-requests` controls concurrently admitted HTTP requests; the default
is 256, and the CLI accepts 1–16384. This is an in-flight admission limit, not a QPS
or bandwidth quota. Requests beyond it return 429/FlowExceeded and `Retry-After: 1`
before parsing or executing SQL, creating sessions, or mutating data. The slot is
released when the admitted handler exits, including cancellation and failure.

For pressure journeys, size admission capacity for the total client fan-out, including
SDK/background workers and multiple sinks. Set a low value in a separate throttling
journey to verify rejection and retry. Fault injection can independently emit bounded
429 responses with the normal protocol error body. Test write metrics count only
admitted requests; rejected arrivals do not inflate data-upload concurrency evidence.
