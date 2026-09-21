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

## Supported capabilities

| Area | Implemented subset |
| --- | --- |
| SQL | CREATE/DROP/TRUNCATE TABLE, INSERT INTO/OVERWRITE, SELECT/WITH; ODPS grammar validation with DuckDB execution |
| Metadata | Table identity, schema, primary keys and properties; table listing; STRING partition create/delete/exists/list/pagination; persistent empty partitions |
| Resources and functions | File-like resource upload (single payload or Java SDK part + merge), metadata read, download with `rOffset`/`rSize`, update, delete, prefix/paginated listing; TABLE resource metadata; Java/SQL/embedded function registration referencing existing resources |
| Tunnel | Protobuf and Arrow batch upload/download; stream upload; Upsert/delete/partial updates; instance result download |
| Storage API v2 | Arrow read/write, Batch/BatchCompatible/Streaming/StreamingRealtime sessions, commit/abort, projections and partition selection |
| Types | Integers, floating point, BOOLEAN, STRING/BINARY, DECIMAL (precision ≤38), DATE/DATETIME/TIMESTAMP, ARRAY/MAP/STRUCT |
| Correctness checks | CRC, schema validation, session isolation, transactional commits, bounded retry tracking and stale-table write rejection |

See [data transfer](docs/data-transfer.md) and [protocol details](docs/protocol.md) for exact combinations and limits. Java SDK Storage v2 uses `/api/storage/v3`; both v2 and v3 paths are accepted.

## Scope and limitations

This is a local/CI test service. By default authentication signatures and permissions are **not validated**; optional strict mode uses local test credentials; keep it on a trusted test network. It is not a replacement for real MaxCompute acceptance tests.

Unsupported: Storage v1, Volume/Blob, CDC/incremental reads, filter predicate pushdown, explicit Schema management, column schema evolution, UDF **execution** (functions are registered metadata; SQL `CREATE FUNCTION`/`DROP FUNCTION` and calling a UDF return `UnsupportedFeature`), volume-backed resources, distributed scheduling, and full ODPS SQL semantics.

Resource payloads are capped at 64 MiB each and 512 MiB per project/schema. Resource and function names resolve case-insensitively while the uploaded spelling is what listings return. Unsupported operations return errors rather than cloud behavior being assumed.

Flink 1.16.2 standalone bounded uploads were tested with Protobuf/Arrow and ordinary/dynamic-partition tables. Use `sink.standalone.enable=true`. A coordinator-mode bounded input in the tested connector can finish without committing rows; that mode is not certified. Checkpoint recovery and cross-job exactly-once have not been validated.

ClickHouse-facing Tunnel behavior is covered by protocol and Java SDK tests. A complete ClickHouse engine integration run is not claimed.

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
```

Native builds require the Go version in `go.mod` and a C/C++ compiler (CGO). The Dockerfile builds inside Linux. [Build notes](docs/build.md) describe module proxies and the optional Zig cross-build path. Generated ANTLR sources are checked in; Java is not needed to build the server. See [grammar provenance](grammar/README.md).

Bug reports should include the SDK/connector version, a minimal fixture, expected/actual rows and a request ID. Please use dummy credentials and synthetic data.

Licensed under Apache-2.0. Third-party attribution: [NOTICE](NOTICE) and [licenses](docs/third-party-licenses.txt).

[Reliability controls](docs/reliability.md): JSON session logs, bounded protocol faults, named quotas, optional strict authentication and SQL type fixtures.
