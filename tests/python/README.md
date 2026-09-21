# PyODPS contract probe

`run.py` drives the emulator through **PyODPS** instead of raw HTTP, so it covers
the request shapes the Java acceptance suite in [../java](../java) never produces:
single-payload resource upload, PyODPS' own `*.part.tmp.*` chunked upload,
offset reads with `x-odps-resource-has-remaining`, `curr_schema` as a query
parameter, and the `NoSuchObject` 404 that `exists()` depends on.

It is a probe of the local emulator against synthetic fixtures, not a MaxCompute
client test: no real project, no credentials, no customer data.

## Run

```bash
go build -o /tmp/emulator ./cmd/emulator
/tmp/emulator --listen 127.0.0.1:8080 --project test_project &
pip install -r tests/python/requirements.txt
python tests/python/run.py http://127.0.0.1:8080 test_project
```

The acceptance workflow (`.github/workflows/ci.yml`) runs this probe against the
same `maxcompute-emulator:ci` image the Java suite tests, on `127.0.0.1:8099`, so
the Python-side wire shapes cannot rot between releases. A full run is ~1 s against
a warm instance (30 cases, 30 passed / 0 failed is the expected line).

Exit code is non-zero if any assertion fails. Every object it creates is named
`pyodps_probe_*` and is deleted again, so the probe can be run repeatedly against
one instance.

## Baseline behaviour to expect

Run it against a build of the released `v1.1.0` tag: the resource and function
plane did not exist there, so almost every case fails with `NoSuchObject`. On a
build that includes the metadata plane the same command must report
`30 passed, 0 failed`. A probe that passes on both is not testing anything.

## Two client-side caveats this probe documents (emulator behaviour is correct)

Both were found by running PyODPS 0.13.2 against the emulator; they are PyODPS
issues, and the probe asserts that the emulator refuses rather than accommodates
them:

1. `create_resource(name, "file", fileobj=<bytes>)` with a payload larger than
   `options.resource_chunk_size` sends a merge manifest that lists **no parts**
   (PyODPS' `_upload_with_stream` calls `.read()` on a `bytes` object, and the
   resulting `AttributeError` is masked by the error raised in `__exit__`).
   The emulator answers `400 InvalidParameter: merge requires at least one part
   resource` and publishes nothing. A lenient `200` would have silently truncated
   the resource.
2. Reusing a cached `FileResource` object after a chunked upload carries
   `merge_total_bytes` on that object, so the *next* plain write on the same
   object is sent as `rOpMerge` with the raw payload as the body. The emulator
   rejects it (`merge body must be <md5>|<part>[,<part>...]`).
   Re-fetching the resource avoids the stale attribute.

Also asserted: the `x-odps-resource-size` and `Content-MD5` reported by `meta`
are the merged payload's, never the merge manifest's.
