MaxCompute Emulator 1.2.0 adds Connector-facing batch read contracts and write recovery test controls.

- Storage v1 batch read sessions, row ranges/resume, projection and Arrow/V5 negotiation.
- Preserve nested timestamp units and Arrow IPC ZSTD/LZ4_FRAME compression.
- Unicode CHAR/VARCHAR contracts, atomic rename/multi-partition updates and explicit partial-PK-update capabilities.
- Test-only Tunnel/Storage write-stage HTTP errors, delays and lost acknowledgments after successful mutation; separate data-upload concurrency metrics.
- SDK/JDBC offline and interactive SQL result paths, resource/function metadata and deterministic retry/authorization contracts.

```sh
docker pull --platform linux/amd64 maxcompute/maxcompute-emulator:1.2.0
docker run --rm --platform linux/amd64 -p 127.0.0.1:8080:8080 \
  maxcompute/maxcompute-emulator:1.2.0 --listen 0.0.0.0:8080 --seed /opt/emulator/seed.sql
```

Use project `test_project` and the mapped port for both service and Tunnel endpoints.
Pin the published image digest for reproducible consumer CI. The image is Linux AMD64;
Apple Silicon runs it through Docker platform emulation. Fault APIs require opt-in
`--test-mode --test-network` and a confined test network.

Validation on the merged feature: Go race/vet, 48 Java SDK checks, 3 legacy SDK,
3 JDBC, 33 PyODPS and the C++ Arrow/fault probe passed. The release workflow rebuilds
from this tag, runs Go/SDK/C++ before publication and reruns SDK acceptance against
the remote-pulled image. See the release Actions run for this exact tag's results.

Compatibility changes: partial PK updates now require
`acid.partial.fields.update.enable=true`. Storage v2 SDK's default
`CrossPartition=true` continues to read the logical snapshot; physical split
planning remains unsupported. Existing consumers should review these contracts.

Limits: Storage v1 writes/physical splits/incremental reads, CDC/DLMV, Volume/Blob,
production IAM/quota/throughput and cross-node recovery remain outside the model.
Transfer sessions and retry history are process-local. Connector append recovery
is at-least-once; passing local checkpoint tests does not certify distributed cloud
recovery or arbitrary end-to-end exactly-once behavior. See
[Connector contracts](https://github.com/dingxin-tech/maxcompute-emulator/blob/v1.2.0/docs/connector-conformance.md).
