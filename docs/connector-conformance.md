# Connector-facing protocol contracts

These changes are on the development branch and are not in the published 1.1.0
image. Build a candidate from the repository [Dockerfile](../Dockerfile) and record
its source commit and image identity. They support local SDK/Connector correctness
tests; they do not reproduce the distributed MaxCompute service.

## Storage v1 batch reads

The v1 adapter uses the existing snapshot engine and serves batch read-session
creation/reload and Arrow data reads. It validates Arrow/V5 negotiation,
case-insensitive format/version spelling, requested row ranges, resume offsets,
projection, and batch sizes from 1 to 20000. Arrow timestamps retain second,
millisecond, microsecond or nanosecond units, including nested values. ZSTD and
LZ4_FRAME use Arrow IPC buffer compression.

Physical bucket/parallelism splitting, cross-partition v1 planning, v1 writes and
incremental/CDC reads remain unsupported and fail explicitly. A single logical
snapshot does not model physical service splits or distributed scheduling.

## Schema and metadata

- `CHAR(n)` accepts lengths 1–255 and `VARCHAR(n)` 1–65535. Values truncate by
  Unicode code points; CHAR trims trailing ASCII spaces. Tunnel character fields
  use binary representation and Storage character fields use UTF-8 with logical
  type and specified length metadata.
- Table rename is transactional and retains identity, rows, partitions and properties;
  target collisions fail without changing either table.
- Multiple `ADD IF NOT EXISTS PARTITION` specifications execute atomically.
- Partial primary-key updates require `acid.partial.fields.update.enable=true`.
  Without that property, partial updates are rejected.
- Explicit `write.bucket.num` metadata supports 1–1024 logical buckets (default 1),
  advertised to SDK upsert slot assignment. The 16-bucket contract is independently
  tested. Slots route to this single worker; this does not implement distributed
  physical bucket storage or split planning.

## Independent verification

Go contracts check timestamp units/compression, v1 validation and row boundaries,
Unicode character normalization, rename/partition atomicity, partial-update
capabilities, strict write authorization and protocol retry identity. Write-fault
contracts independently verify that successful mutations are visible before a retry,
that repeated stream identity does not duplicate a pack, and that repeated commit
acknowledgment does not republish rows.

The companion Flink SQL suite uses SDK 0.61.2-public and Flink 1.20.4 with independent
Tunnel readback. The 2026-10-13 clean local run passed 165 offline and 166 Emulator
checks, including 18 concurrent-write cases and 58 fault cases, plus 6 checkpoint
recovery scenarios. The larger stress run wrote 884736 logical rows at parallelism
4/8/16 and measured a peak of 16 simultaneous data uploads. Append recovery still
permits duplicates; PK checks assert final values and unique keys. The suite's
80.53% line coverage measures the Connector, not this Emulator.

These results used local image
`sha256:d1240df999fc8d1c97489350724a0d72120e1fe114e17ee8f3c44174c190c8d8`;
it is not a published registry digest. Archive clean logs and source manifests with
new runs. See [build](build.md) and [fault controls](reliability.md).

Production IAM/quota/throughput, CDC/DLMV, cross-node recovery and physical split
planning need separate real-service acceptance. Transfer sessions and retry history
remain process-local across Emulator restarts.
