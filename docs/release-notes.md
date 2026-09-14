The first community release of the Go + DuckDB rewrite.

- SQL fixtures, table metadata, durable table IDs and STRING partition lifecycle.
- Tunnel Protobuf/Arrow uploads and downloads, streaming upload, Upsert and instance results.
- Storage API v2 Arrow reads/writes, session lifecycle and transactional batch commit.
- CRC, bounded retry tracking, stale-session isolation and graceful shutdown.
- Java SDK 0.61.2-public acceptance with Testcontainers.

```bash
docker pull maxcompute/maxcompute-emulator:1.0.0
docker run --rm --platform linux/amd64 -p 127.0.0.1:8080:8080 \
  maxcompute/maxcompute-emulator:1.0.0 --listen 0.0.0.0:8080 --seed /opt/emulator/seed.sql
```

Images are Linux amd64. Configure ODPS and Tunnel endpoints to the mapped port, project `test_project`, schema `default`, and dummy credentials.

**Migration:** `main` is now Go; the previous Spring Boot/SQLite implementation is preserved on `legacy`. Existing old image tags remain available. SQLite databases are incompatible; recreate SQL fixtures. Transfer sessions and retry history are not persisted across restarts.

**Scope:** local/CI testing; authentication signatures and permissions are not validated. Storage v1, Volume/Blob, CDC, explicit Schema management and column schema evolution are not supported. Flink standalone bounded uploads were verified; the tested connector's coordinator mode can finish without committing, and checkpoint recovery is not certified. Full ClickHouse engine integration is not claimed.

See README and docs/release-readiness.md for the complete compatibility matrix.
