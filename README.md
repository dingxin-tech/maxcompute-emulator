# MaxCompute Emulator

[简体中文](README.zh-CN.md) · [Docker Hub](https://hub.docker.com/r/maxcompute/maxcompute-emulator) · [Compatibility](docs/release-readiness.md) · [Changelog](CHANGELOG.md)

Run a local MaxCompute-compatible service for SDK development and integration tests. Version **1.1.0** is a Go + DuckDB rewrite with SQL fixtures, table and partition metadata, Tunnel Protobuf/Arrow transfers, and Storage API v2.

`main` contains the Go implementation. The previous Spring Boot + SQLite implementation is preserved on [`legacy`](https://github.com/dingxin-tech/maxcompute-emulator/tree/legacy); its existing image tags remain available.

## Quick start

Docker images target **Linux amd64**. On Apple Silicon, use Docker's amd64 emulation.

```bash
docker pull maxcompute/maxcompute-emulator:1.1.0
docker run --rm --name mc-emulator --platform linux/amd64 \
  -p 127.0.0.1:8080:8080 maxcompute/maxcompute-emulator:1.1.0 \
  --listen 0.0.0.0:8080 --seed /opt/emulator/seed.sql
curl -fsS http://127.0.0.1:8080/readyz
```

Use `http://127.0.0.1:8080` for both the ODPS endpoint and Tunnel endpoint, project `test_project`, schema `default`, and dummy credentials such as `test-ak` / `test-sk`. The seed creates `demo` and partitioned `events` tables. Omit `--seed` to start empty.

Pin `1.1.0` for reproducible tests. `latest` follows stable releases and may change.

## Java SDK and Testcontainers

Use `com.aliyun.odps:odps-sdk-core:0.61.2-public` and Testcontainers. A complete Maven project and 25 acceptance tests are in [tests/java](tests/java).

```java
try (GenericContainer<?> mc = new GenericContainer<>(
        DockerImageName.parse("maxcompute/maxcompute-emulator:1.1.0"))
        .withExposedPorts(8080)
        .waitingFor(Wait.forHttp("/readyz"))) {
    mc.start();
    String endpoint = "http://" + mc.getHost() + ":" + mc.getMappedPort(8080);
    Odps odps = new Odps(new AliyunAccount("test-ak", "test-sk"));
    odps.setDefaultProject("test_project");
    odps.setCurrentSchema("default");
    odps.setEndpoint(endpoint);
    odps.setTunnelEndpoint(endpoint);
    SQLTask.run(odps, "create table sample(id bigint, name string)").waitForSuccess();
    SQLTask.run(odps, "insert into sample values(1,'hello')").waitForSuccess();
    TableTunnel tunnel = new TableTunnel(odps);
    tunnel.setEndpoint(endpoint);
    TableTunnel.DownloadSession session =
        tunnel.createDownloadSession("test_project", "sample");
    try (TunnelRecordReader reader = session.openRecordReader(0, 1)) {
        System.out.println(reader.read().getString(1)); // hello
    }
}
```

With JDK 17, Maven and Docker installed:

```bash
mvn -B -f tests/java/pom.xml \
  -Demulator.image=maxcompute/maxcompute-emulator:1.1.0 test
```

Arrow on JDK 17 requires `--add-opens=java.base/java.nio=ALL-UNNAMED`; the test POM sets it. Both endpoints must use the mapped port. Between containers on one Docker network, use the emulator's container DNS name instead of `localhost`.

## JDBC

Point the official driver at the emulator with the same endpoint, project and dummy
credentials:

```
jdbc:odps:http://127.0.0.1:8080?project=test_project&accessId=test-ak&accessKey=test-sk&tunnelEndpoint=http://127.0.0.1:8080
```

`tests/jdbc` runs the published driver (currently `odps-jdbc` 3.10.13, which bundles
`odps-sdk-core` 0.58.1) against the image, in offline mode:

```bash
mvn -B -f tests/jdbc/pom.xml \
  -Demulator.image=maxcompute/maxcompute-emulator:1.1.0 test
```

The driver signs a logview token and opens a Tunnel download session for **every**
statement, including `CREATE TABLE` and `INSERT`, which is a stricter contract than the
SDK path exercises; that is where these checks live. MCQA (`interactiveMode=mcqa`) needs the
session plane; `interactiveMode=maxqa` is refused by name.


## Supported capabilities

| Area | Implemented subset |
| --- | --- |
| SQL | CREATE/DROP/TRUNCATE TABLE, INSERT INTO/OVERWRITE, SELECT/WITH; ODPS grammar validation with DuckDB execution |
| Metadata | Table identity, schema, primary keys and properties; table listing; STRING partition create/delete/exists/list/pagination; persistent empty partitions |
| MCQA session (SQLRT) | `Instance/Job/Tasks/SQLRT` creates a session instance that stays `Running` until the client stops it or it idles out; statements arrive as sub queries over the instance information KV (`?info&taskname=...`), with `status` / `progress` / `result_<id>` reads, `query` / `cancel` writes, and the object status codes the Java SDK polls on. A sub query's result is also downloadable over the instance tunnel (`?data&cached&taskname=...&queryid=N`), which is the Java SDK's default fetch and JDBC MaxQA's read path |
| Resources and functions | File-like resource upload (single payload or Java SDK part + merge), metadata read, download with `rOffset`/`rSize`, update, delete, prefix/paginated listing; TABLE resource metadata; Java/SQL/embedded function registration referencing existing resources |
| Tunnel | Protobuf and Arrow batch upload/download; stream upload; Upsert/delete/partial updates; instance result download |
| Storage API v2 | Arrow read/write, Batch/BatchCompatible/Streaming/StreamingRealtime sessions, commit/abort, projections and partition selection |
| Types | Integers, floating point, BOOLEAN, STRING/BINARY, DECIMAL (precision ≤38), DATE/DATETIME/TIMESTAMP, ARRAY/MAP/STRUCT |
| Correctness checks | CRC, schema validation, session isolation, transactional commits, bounded retry tracking and stale-table write rejection |

See [data transfer](docs/data-transfer.md) and [protocol details](docs/protocol.md) for exact combinations and limits. Java SDK Storage v2 uses `/api/storage/v3`; both v2 and v3 paths are accepted.

## Scope and limitations

This is a local/CI test service. By default authentication signatures and permissions are **not validated**; optional strict mode uses local test credentials; keep it on a trusted test network. It is not a replacement for real MaxCompute acceptance tests.

Offline SQL instance results (`?result`) also include the CSV column-name header expected by `SQLTask.getResult`. Empty SELECTs retain that header, while statements with no result schema return an empty result. The legacy Java SDK regression can be run with `mvn -B -f tests/java/pom.xml -Plegacy-sql-sdk -Demulator.image=maxcompute-emulator:dev clean test`.

MCQA sessions cover both of the Java SDK's read paths: the information channel (`SQLExecutor` with `useInstanceTunnel(false)`) answers with CSV whose first line is the column-name header, and the instance tunnel (`?data&cached&taskname=..&queryid=..`, the default fetch and the one JDBC MaxQA uses) answers with a record stream that carries its own schema, so a sub-query result arrives with the same rows and the same types either way. `instance_tunnel_limit_enabled` applies the service's `READ_TABLE_MAX_ROW` (10,000 rows) to that read, `rowrange` pages through it, and `sizelimit` shortens a response instead of failing it. Statements that produce no result set are refused on the tunnel with `InstanceTypeNotSupported`, like the service, so a client confirms them through the session API rather than reading an empty stream. A session is **select-only by default**: a `CREATE`/`INSERT`/`DROP` submitted as a sub query is declined before anything runs (`queryId: -1` plus `ODPS-1850001 Non select query not supported.`), because the Java client reacts to a non-select it learns about at fetch time by running the same statement offline again — and an INSERT that already ran in the session would land twice. Send `odps.sql.session.select.only=false`, in the session settings or per statement, to run them in the session anyway. Session-scoped state (variables, temp objects) is not isolated — statements run against the shared engine. Not simulated: named-session attach (a session name is metadata; equal names create separate sessions), MaxQA v2 (`/mcqa` request prefix), compressed or Arrow-encoded sub-query downloads (Protobuf records only), and per-statement statistics.
Unsupported: Storage v1, Volume/Blob, CDC/incremental reads, filter predicate pushdown, explicit Schema management, column schema evolution, UDF **execution** (functions are registered metadata; SQL `CREATE FUNCTION`/`DROP FUNCTION` and calling a UDF return `UnsupportedFeature`), volume-backed resources, distributed scheduling, and full ODPS SQL semantics.

Resource payloads are capped at 64 MiB each and 512 MiB per project/schema. Resource and function names resolve case-insensitively while the uploaded spelling is what listings return. Unsupported operations return errors rather than cloud behavior being assumed.

Two measured differences from the service - the codes a refused chunked merge returns, and a `TABLE` resource accepted for a table that does not exist - are listed in [Known divergences from the real service](#known-divergences-from-the-real-service).

Flink 1.16.2 standalone bounded uploads were tested with Protobuf/Arrow and ordinary/dynamic-partition tables. Use `sink.standalone.enable=true`. A coordinator-mode bounded input in the tested connector can finish without committing rows; that mode is not certified. Checkpoint recovery and cross-job exactly-once have not been validated.

ClickHouse-facing Tunnel behavior is covered by protocol and Java SDK tests. A complete ClickHouse engine integration run is not claimed.

## Known divergences from the real service

Two differences against a live MaxCompute project were **measured on the wire**, not inferred, and they
stay as they are on purpose: the codes below are what this repository's own tests assert on, so changing
one is a contract decision, not a documentation fix. Until that decision is taken, this section is the
tracking list — if your measurement contradicts a row, open an issue with both readings and a request id.

Both rows describe `main`; the released image is outside both of them. Checked directly against
`maxcompute/maxcompute-emulator:1.1.0` (digest `sha256:57d1d25757255a1f16d0a3c34ef8b99c24bf8615e1c14c13dfd5a41026dee2d3`,
2026-10-08): `GET /projects/<project>/resources` answers `404` with `UnsupportedOperation: unsupported
endpoint`, and `/capabilities` declares neither `resources` nor `functions`. The PyODPS contract probe
against that image ends at `1 passed, 32 failed` - and the single case that passes there passes because
the whole endpoint is refused. Same shape as the 2026-09-21 run against a `1.1.0` source build
(`1 passed, 29 failed`): the case count grows with the probe, the missing-capability answer does not.
So there is nothing to diverge from on `1.1.0` - use resources only from a source build or a tag after it.

### 1. A refused merge comes back with different status and error codes

Chunked resource uploads finish with a `?rOpMerge=true` request. The Java SDK takes that path for
anything overflowing its 64 MiB chunk buffer - and, whatever the size, for a stream whose length it
cannot determine (a pipe or a network stream); PyODPS takes it above `options.resource_chunk_size`.
Two refusal shapes were compared:

| The merge is refused because | Live service | Emulator |
| --- | --- | --- |
| the assembled payload does not match the MD5 in the merge body | `500 InternalServerError` — `ODPS-0421213: Save resource error - Merge part temp files failed! Message: The merged file's signature does not match!` | `400 InvalidParameter` — `merged payload does not match MD5 <md5>` |
| the target resource name already exists | `409 ObjectAlreadyExists` — `ODPS-0421121: The resource has already existed - <name>` | `400 ResourceAlreadyExists` |

Everything else about those refusals agrees, and that is the part worth depending on: a merge that fails
after assembling the payload consumes the parts it listed, so a retry re-uploads them; a refusal settled
before any part is read leaves the parts addressable and does not touch the existing target; a manifest
naming a part that was never uploaded is `404 NoSuchObject` on both sides. Only the *shape* of the
refusal differs.

Three more things the *same* request hits, measured against a live project and against `main` on
2026-10-08 (all three are refusal-mechanics differences, so they belong to this row rather than to a new one):

| Same `?rOpMerge` request | Live service | Emulator |
| --- | --- | --- |
| without `x-odps-resource-merge-total-bytes` | `400 InvalidParameter` — `ODPS-0420051: Missing header in HTTP request - x-odps-resource-merge-total-bytes`; the header is **required** | the header is optional: the merge is attempted and refused for the target instead (`400 ResourceAlreadyExists`) |
| a wrong declared total **and** an existing target | `409 ObjectAlreadyExists` — the target check runs first and the declared total is never compared | `400 InvalidParameter` on the declared total — this emulator's own check runs first |
| the part a refusal-before-reading leaves behind | addressable by name (`?meta` 200, delete works) but **absent from listing**, including an exact-name prefix query | addressable by name **and listed** |

**Who sees it, and what to do:** tests or retry policies that branch on the HTTP status or on the client
exception class. In the cloud the two refusals arrive as a server error (`InternalServerError`, retryable)
and a conflict (`ObjectAlreadyExists`); locally both arrive as one `400`, so an emulator-backed test of
"500 retries, 400 does not" is testing this server's taxonomy instead of the service's. Assert that the
merge was refused and assert the part/target state — `go test ./internal/server -run
TestResourceRESTContract` pins exactly those - **by name** (`Resource.exists()` / `?meta`), never by
listing: a cleanup assertion built on `list_resources(prefix=...)` reads clean against the cloud while
the part still exists, and reads leaked against the emulator when nothing leaked. Keep numeric status assertions out of emulator-backed tests,
or pin them per target and say so in a comment.

### 2. A `TABLE` resource may point at a table that does not exist

| Case | Live service | Emulator |
| --- | --- | --- |
| create a `TABLE` resource whose source table was never created | `404 NoSuchObject` — `ODPS-0422111: Table not found - <project>.<table>`; the resource is not created | `201 Created`; the resource is listed and `?meta` returns its `TableName` |

**Who sees it, and what to do:** any fixture that registers a table-backed resource, or a function that
depends on one, with a typo'd or not-yet-created table name. It passes locally and fails in the cloud —
and there it fails on the *create* call, not at first use, so a suite that only ever ran against the
emulator reports a green setup for a reference the service never accepts. The emulator requires a
non-empty `x-odps-copy-table-source` and does validate the *resources* a function references; the table
behind a `TABLE` resource is the one pointer it does not resolve. Create the table in the seed, or assert
`odps.exist_table(...)` / `odps.tables().exists(...)` in the fixture setup, so a broken reference fails
locally for the reason it would fail remotely.

### What these readings do not cover

Both rows come from **one live project**, read from the client-visible layer only (PyODPS exception
`status_code`, `code`, message text), against an `http://` service endpoint, on Linux/amd64, between
2026-10-02 and 2026-10-05; the emulator column was re-measured against `main` at `24f3fce`. Stated as
unverified rather than assumed:

- the same cells in another region, on another service version, or behind public HTTPS with a real
  certificate chain — no run here went through TLS termination or a gateway, so gateway-added error
  shapes are unknown;
- a BSD or macOS native build: the emulator numbers above were read from a Linux/amd64 binary (image or
  local build). Apple Silicon runs that same Linux image under emulation, which is a different check and
  was not performed for these rows;
- other merge refusal shapes (malformed merge body, oversized part, quota refusal) were never compared,
  and the three header/ordering cells above were measured only on the duplicate-target path and the
  missing/wrong declared-total combinations this probe tried - another combination is unmeasured,
  so a further difference is not contradicted by this table - it is simply unmeasured. One exception is
  known and runs the other way: the service ignores a declared `x-odps-resource-merge-total-bytes` that
  disagrees with the assembled payload (re-measured 2026-10-08: 304 bytes merged under a 4400-byte
  declaration, accepted, published byte-exact) while this emulator refuses it. That third difference is
  documented in [the protocol notes](docs/protocol.md) instead of here, because it is the emulator being
  stricter than the service, not a cloud behavior the emulator lacks.

To re-measure the emulator column without a project: `go test ./internal/server -run
TestResourceRESTContract` pins the codes and the part/target state of row 1, and
`python tests/python/run.py http://127.0.0.1:8080` (dummy credentials, synthetic fixtures, wired into
CI) asserts the same two merge refusals through the PyODPS client - the part lifecycle, deliberately
not the service's status numbers. Row 2 has no case in this repository: the Go test registers a
`TABLE` resource against a table that exists, so "created even when the table is missing" is
recorded here from measurement, not from a test.

The service column needs a project of your own: upload a part
(`project.resources.create(name=..., type="file", temp=True, part=True, fileobj=...)`), call
`project.resources.merge_part_files(...)` with an MD5 that does not match the assembled bytes, repeat
the merge against an existing name, then `odps.create_resource(name, "table", table_name="<never created>")`
and print `status_code`, `code` and the message of whatever the client raises. Two runs agreeing on the
same three numbers is what would move rows 1 and 2 from "measured once" to "pinned".

## Fixtures, persistence and limits

Mount SQL fixtures read-only and pass `--seed /fixtures.sql --project your_project`. By default, all data is temporary. For persistence, mount a directory writable by UID 65532 at `/data` and pass `--database /data/emulator.duckdb`.

Committed data and table metadata persist; transfer sessions and retry history do not survive restarts. **Legacy SQLite files are incompatible with DuckDB**: recreate fixtures when migrating. Legacy consumers can keep using a pinned `0.0.x` image.

Defaults: 100,000 rows per result, 64 MiB estimated decoded data per write session, 64 sessions per transfer API, 30-minute session TTL and 16 concurrent requests. Use fresh containers per test suite or configure `--max-sessions`, `--session-ttl` and `--max-rows`. Limits are documented in [compatibility notes](docs/release-readiness.md); they do not constitute a process RSS guarantee.

## Build and contribute

```bash
go test -race ./...
go vet -unreachable=false ./... # generated ANTLR unreachable branches excluded
docker build --platform linux/amd64 -t maxcompute-emulator:dev .
mvn -B -f tests/java/pom.xml -Demulator.image=maxcompute-emulator:dev test
mvn -B -f tests/jdbc/pom.xml -Demulator.image=maxcompute-emulator:dev test
```

Native builds require the Go version in `go.mod` and a C/C++ compiler (CGO). The Dockerfile builds inside Linux. [Build notes](docs/build.md) describe module proxies and the optional Zig cross-build path. Generated ANTLR sources are checked in; Java is not needed to build the server. See [grammar provenance](grammar/README.md).

Bug reports should include the SDK/connector version, a minimal fixture, expected/actual rows and a request ID. Please use dummy credentials and synthetic data.

Licensed under Apache-2.0. Third-party attribution: [NOTICE](NOTICE) and [licenses](docs/third-party-licenses.txt).

[Reliability controls](docs/reliability.md): JSON session logs, bounded protocol faults, named quotas, optional strict authentication and SQL type fixtures.
