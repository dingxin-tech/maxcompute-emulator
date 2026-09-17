ClickHouse protocol fixes and local reliability test controls.

- Fix Tunnel Arrow ZSTD/LZ4 IPC compression and missing project/partition errors.
- Add JSON download lifecycle logs with session hashes and request ranges.
- Add 12 opt-in protocol fault effects with expiration, hit budgets and cancellation.
- Support default/named quotas and QuotaNotExist / FlowExceeded tests.
- Add optional ODPS v2/v4/STS signature, date and local read/write ACL verification.
- Add public SQL TIMESTAMP_NTZ, MAP construction, nested/empty collections and numeric fixtures.

```sh
docker pull maxcompute/maxcompute-emulator:1.1.0
docker run --rm --platform linux/amd64 -p 127.0.0.1:8080:8080 \
  maxcompute/maxcompute-emulator:1.1.0 --listen 0.0.0.0:8080 --seed /opt/emulator/seed.sql
```

Use project `test_project` and point both service and Tunnel endpoints to the
mapped port. Default authentication remains permissive; fault injection is off.
See [reliability controls](https://github.com/dingxin-tech/maxcompute-emulator/blob/v1.1.0/docs/reliability.md) for strict credentials, test-mode
network controls, quota configuration and `/opt/emulator/types.sql`.

**Validation:** Go race/vet in containers, Java SDK 0.61.2-public (33 tests,
including strict Storage reads and row-disconnect recovery), and a pinned public
C++ SDK Arrow wire probe (success, corrupt streams and one-shot recovery).
Publication runs these gates before pushing and reruns Java against the remote
image. Refer to the release Actions run for the published revision's results.

**Limits:** Full ClickHouse E2E has not been rerun for this release. The C++ probe
is a wire-contract test, not the complete SDK HTTP client. Production quota,
IAM/STS, routing and concurrency still require real MaxCompute acceptance.
The Java SDK's Long.MIN_VALUE rejection remains an explicit negative test.
Volume/Blob, Storage v1 and CDC remain unsupported. Session state is ephemeral;
Flink checkpoint recovery is not certified. Retain the original `--project`
when reusing a 1.0.0 DuckDB database; SQLite legacy data cannot be reused.
