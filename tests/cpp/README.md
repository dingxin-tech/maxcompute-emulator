# C++ Tunnel wire contract probe

This probe uses the **unmodified public ODPS C++ SDK** `ArrowInputStream`
(CRC framing) at `a1a7e541753f53970131e345a4f77581a8d6a9ac`, followed by
Arrow C++ 19.0.1 `ReadRecordBatch`, matching the decoding path used by
`ArrowRecordReader`. Python sends HTTP requests with dummy/no credentials.
It is a wire contract test, not the complete SDK HTTP client or ClickHouse E2E.

Start a disposable emulator without production data, listening on
`0.0.0.0:8080` with the default `test_project`. Then on Docker Desktop:

```bash
docker build -t emulator-cpp-contract tests/cpp
docker run --rm emulator-cpp-contract http://host.docker.internal:8080
```

On Linux, add `--add-host host.docker.internal:host-gateway` or run both
containers on one Docker network and use the emulator's container DNS name.
The runner creates/replaces `cpp_contract_0`, `cpp_contract_8` and
`cpp_contract_10007` and `cpp_contract_partition` in `test_project`. Use a fresh test instance.

Checks: identity/ZSTD/LZ4 IPC decoding; empty/single/multiple batches;
exact row count, all column values, order and no duplicates; project/table
404 codes and request ID. Each download is completed in a `finally` block.
SDK CRC and IPC errors fail the probe, as does any nonempty range returning EOF.

The complete consumer gate still requires the real ClickHouse branch,
its E2E runner and final service interoperability checks.
