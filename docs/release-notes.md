MaxCompute Emulator 1.2.1 makes concurrent HTTP admission configurable and corrects throttling semantics.

- Increase default concurrent admission from 16 to 256; `--max-concurrent-requests` accepts 1–16384.
- Reject excess requests with HTTP 429/FlowExceeded and `Retry-After: 1` before mutations.
- Count admitted write requests only, so rejected requests cannot inflate concurrency evidence.
- Add admission boundary, non-mutation, reusable capacity and metric contract tests.

```sh
docker pull --platform linux/amd64 maxcompute/maxcompute-emulator:1.2.1
docker run --rm --platform linux/amd64 -p 127.0.0.1:8080:8080 \
  maxcompute/maxcompute-emulator:1.2.1 --listen 0.0.0.0:8080 \
  --max-concurrent-requests 1024 --seed /opt/emulator/seed.sql
```

Admission capacity is a concurrent-request budget, not a QPS or bandwidth simulation.
Keep intentional throttling tests separate from correctness pressure journeys. No cloud
throughput or memory-usage guarantee follows from raising this budget.
