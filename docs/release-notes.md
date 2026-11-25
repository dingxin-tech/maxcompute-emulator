MaxCompute Emulator 1.2.1 carries everything merged on `main` after 1.2.0 (2026-10-13):
version-window incremental reads for ACID tables, managed Schema APIs, JDBC typed literals and
nested result types, bounded SQL validation with block comments, an HTTP capture proxy,
quota-routed discovery with a rotating test-credential fixture, and configurable concurrent HTTP
admission. It also corrects three refusal semantics toward what the service does.

## New capability

- **Incremental reads (Storage v1 and v2)** over a version window for ACID tables: sessions created
  with `session_type=incremental_read` (v1) or `IncrementalRead=true` plus
  `IncrementalReadOptions` (v2) return the events in `(StartVersion, EndVersion]`, echo the options,
  and publish `LatestVersion` so a reader stores the version it reached and asks only for what
  follows. `cdc` answers what happened from the event history; `append` answers what is new from
  live rows. Events carry the NOT NULL `__meta_op_type` (`tinyint`) and `__meta_timestamp`; the
  version columns stay out of every user-visible projection and schema.
- **Row-level versions on delta tables** (primary key plus `transactional` or format-version 2):
  SQL `INSERT`, Tunnel upload, Storage batch/streaming/upsert commits, `INSERT OVERWRITE`,
  `TRUNCATE` and `ALTER TABLE DROP PARTITION` all append events, so a row inserted and then deleted
  inside one window is gone from `append` (live rows) while `cdc` keeps both its insert (2) and
  delete (4) events - the split is pinned by `TestDeltaSameWindowInsertThenDeleteKeepsBothEventsInCdc`.
- `SELECT get_latest_version('<project>[.<schema>].<table>')` answers in the SQL plane from the same
  bound an incremental session reports as `LatestVersion` - the poll a delta source makes before it
  plans a window.
- **Persistent managed Schema** create/get/list/delete with project isolation, prefix/paginated
  listing, idempotent deletion and cascade removal of managed objects.
- **JDBC typed numeric literals** (`L`, `Y`, `S`, `BD`, `F`) execute with their declared types and
  decimal precision/range checks; nested query result types translate to ODPS ARRAY/MAP/STRUCT
  metadata before Tunnel results are served.
- **SQL validation is bounded**: no unused parse tree is retained, one budget is shared across
  execution and SQLRT admission, ordinary block comments are accepted within the SQL byte limit, and
  executed results are formatted from the executed statement instead of being reparsed.
- **`emulator proxy --upstream <fixed endpoint> [--capture-headers a,b]`** forwards a signed SDK
  request unchanged and writes one JSON line per completed request (method, path without query,
  selected headers, upstream status, Request ID). Credential headers are refused as capture
  choices; bodies and query values never enter the capture.
- **Named-quota discovery** returns an endpoint under `/__quota/<name>` that carries the quota for
  later requests without touching the signed resource path, and a **test STS rotation fixture**
  (`POST /__test/sts`, `credentials_uri` path, rotate/revoke) provides rotating temporary
  credentials when `--test-mode` and strict authentication are both on.
- The published **Go SDK** is covered by a regression suite (`bash tests/go/verify.sh IMAGE`,
  `database/sql` plus resumable Protobuf Tunnel against `aliyun-odps-go-sdk` v0.4.26), now a CI step.

## Behavior changes to check before upgrading

- **Throttling shape changed.** Above the admission budget a request now answers
  `429`/`FlowExceeded` with `Retry-After: 1` before SQL parsing, session creation or any mutation;
  1.2.0 answered `503`/`ServiceUnavailable`. A consumer that retries only on 5xx, or that branches
  on the error code string, needs re-checking. The default concurrent budget went 16 -> 256;
  `--max-concurrent-requests` accepts 1-16384 and an out-of-range value fails startup. Test-mode
  write metrics count admitted requests only, so rejected arrivals can no longer inflate the
  concurrency numbers a pressure journey asserts on.
- **Two incremental window shapes, and only the first one matches the service.** They are worth
  separating because a retry rule written against one of them is wrong for the other.
  - **Past the version the table has reached** (`EndVersion`, or `StartVersion`, above the bound a
    session pinned) is refused here - `400`/`InvalidParameter` - rather than served or capped:
    `LatestVersion` is the cursor a consumer stores, so handing back a number the table never
    reached silently skips every version up to that bound. Measured against a live ACID table at
    version 2 (2026-11-12), `(0,2]` answers its two events while `(0,3]` and `(0,1002]` both fail
    with `The query version exceeded timetravel range`; reading the service's own refusal on
    2026-11-14 by walking the cause chain under the `IOException` gives an `OdpsException` with
    HTTP status `400` and error code `InvalidArgument`. The status matches this emulator; the code
    string stays `InvalidParameter` on purpose - that is the invalid-value family across the storage
    face, and `InvalidArgument` appears nowhere in this repository. So a consumer that branches on
    the code string, not the status, sees one difference here.
  - **An inverted window (`StartVersion` above `EndVersion`) is the known divergence, not the case
    above**: the same 2026-11-12 measurement shows the service accepts `(2,1]` and answers an empty
    window at version 1, while this emulator refuses it with `400`/`InvalidParameter` instead of
    returning a cursor below the reader's own start.
- **Storage-face refusals of unsimulated capabilities now answer `405`/`MethodNotAllowed`**
  uniformly (unknown `WriteMode`, filter-predicate pushdown, bucket/parallelism/bucket-sort split
  planning, an `Accept-Encoding` codec outside the advertised set, `data_format_type`/
  `data_format_version` outside Arrow V5, a zero batch size), so a consumer can tell "not provided
  here" from "you sent something invalid" (`400`/`InvalidParameter`, unchanged). Other faces still
  answer `400`/`UnsupportedOperation`.
- **Storage v1 read sessions accept `CrossPartition`** instead of refusing it; the public table API
  that the Flink/Spark connectors build on defaults the flag to true, and every emulator read is one
  logical split that already spans the selected partitions.
- **Storage write-stream IDs must match the production format `[A-Za-z0-9-]+`.** 1.2.0 accepted any
  non-empty string, so stream names like `worker_1`, `worker.1` or `worker/1` now get
  `400`/`InvalidParameter` instead of a usable stream.
- **A submitted SQL task honours `odps.default.schema`** from its `Config`/`Property` settings, so
  unqualified table names resolve in the schema the client declared rather than the one on the
  instance route. Malformed settings JSON is refused `400`/`InvalidParameter`.

```sh
docker pull --platform linux/amd64 maxcompute/maxcompute-emulator:1.2.1
docker run --rm --platform linux/amd64 -p 127.0.0.1:8080:8080 \
  maxcompute/maxcompute-emulator:1.2.1 --listen 0.0.0.0:8080 \
  --max-concurrent-requests 1024 --seed /opt/emulator/seed.sql
```

## What this release still does not simulate

Timestamp-window incremental reads, `StartTxnId`, filter predicates over an incremental window,
physical bucket and parallelism split planning, v1 write sessions, Volume/Blob, distributed
transaction recovery and cross-job exactly-once. Each of these is refused with the service's own
status rather than answered with an empty success, and `/capabilities` names the gaps.
Admission capacity is a concurrent-request budget, not QPS, bandwidth or a memory guarantee.

## How a candidate is verified

Build and acceptance use the patched Go toolchains (`golang:1.27.4-bookworm`, `go 1.27.4`;
`golang:1.26.11-bookworm` for the Go consumer image).

On every push to `main` and every pull request, CI runs Go `-race` and `go vet` inside the build
image, builds the candidate image, then the published Go SDK consumer suite, the Java SDK
acceptance suite, the legacy offline SQL-result suite, the published JDBC driver suite, the PyODPS
resource/function contract probe and the C++ Arrow wire contract and fault tests.

Pushing a `v*` tag runs a narrower gate, in this order:

1. the tag number must match `const Version` in `internal/server/server.go`;
2. Go `-race` and `go vet` inside the build image, the Linux source image build (labelled with the
   revision), the Java SDK acceptance suite against that locally built image, and the C++ Arrow
   contract and fault tests - all of it before anything is published;
3. the version image is pushed, the local copy is removed, the image is pulled back **from the
   registry**, and the Java SDK acceptance suite runs again against the pulled image;
4. only if that passes is `latest` moved to the same image;
5. then the GitHub Release is created from this file
   (`gh release create --verify-tag --notes-file docs/release-notes.md`).

The consequence to plan for: if the step-3 acceptance fails, the version image is already visible in
the registry, but `latest` does not move and no GitHub Release is created. That half-published state
needs a decision on the same commit - re-running or rolling the tag back - not a push of a different
commit onto the same version number.

The Go consumer, JDBC and PyODPS steps exist only in CI (`ci.yml`): they are not part of the tag
workflow, so run them for the commit you intend to tag before announcing. `.github/workflows/package.yml`
produces the offline archive (tar.gz, SHA-256 and the image descriptor) after the release exists; it
verifies the published digest against the source tag, fails when `docker save` fails instead of
checksumming a partial archive, and its `version`/`digest` inputs still default to 1.2.0 - pass
1.2.1 and the accepted registry digest explicitly.

[Build notes](build.md) and [release readiness](release-readiness.md) carry the scope; the emulator is a
local/CI test service and does not replace real MaxCompute acceptance.
