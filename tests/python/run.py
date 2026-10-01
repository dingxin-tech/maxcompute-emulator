#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PyODPS consumer contract probe for the resource / function metadata plane.

Local emulator only: synthetic fixtures, no customer data, no credentials.
This is the Python counterpart of ../java (UdfMetadataTest) and it deliberately
covers the wire shapes the Java SDK never produces:

  * a single-payload upload (str / bytes / BytesIO), which is what PyODPS sends
    for anything below ``options.resource_chunk_size``;
  * PyODPS' own part names ``<resource>.part.tmp.<rand>.<index>`` for the
    chunked upload path, merged with ``rOpMerge`` and ``x-odps-resource-merge-total-bytes``;
  * chunked *reads* through ``rOffset`` / ``rSize`` and the
    ``x-odps-resource-has-remaining`` flag PyODPS uses to decide EOF;
  * ``curr_schema`` as a query parameter (Java sends it, PyODPS relies on it);
  * the parseable 404 that ``Resource.exists()`` / ``Function.exists()`` catch on.

Two refusals are asserted on purpose (section 7). Real PyODPS 0.13.2 can emit
exactly those two malformed merge requests when its own object cache is reused or
when a ``bytes`` payload is larger than the configured chunk size; if the
emulator answered 200 there, a truncated resource would be published silently and
local runs would pass where the service fails.

Usage:  python run.py http://127.0.0.1:8080 [project]
Exits non-zero when any assertion fails.
"""
import hashlib
import io
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager

from odps import ODPS, options

ENDPOINT = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8080").rstrip("/")
PROJECT = sys.argv[2] if len(sys.argv) > 2 else "test_project"
PREFIX = "pyodps_probe"
SCHEMA = "probe_schema"
CHUNK = 1024

passed, failed = [], []


def case(name, fn):
    expect_error = name.startswith("rejects")
    try:
        detail = fn()
        if expect_error:
            failed.append((name, "expected a refusal, got success: %s" % detail))
            print("FAIL %s :: expected refusal, got %s" % (name, detail))
        else:
            passed.append(name)
            print("PASS %s :: %s" % (name, str(detail)[:170].replace("\n", " ")))
    except Exception as exc:  # noqa: BLE001 - a probe reports, it does not retry
        if expect_error:
            passed.append(name)
            print("PASS %s :: refused with %s" % (name, str(exc)[:150].replace("\n", " ")))
        else:
            failed.append((name, "%s: %s" % (type(exc).__name__, exc)))
            print("FAIL %s :: %s: %s" % (name, type(exc).__name__, str(exc)[:220].replace("\n", " ")))


def expect_refusal(name, fn, needle):
    """fn() must raise an ODPS error whose text mentions `needle`."""
    try:
        fn()
    except Exception as exc:  # noqa: BLE001
        if needle not in str(exc):
            failed.append((name, "refused for the wrong reason: %s" % exc))
            print("FAIL %s :: wrong reason: %s" % (name, str(exc)[:180].replace("\n", " ")))
        else:
            passed.append(name)
            print("PASS %s :: %s" % (name, str(exc)[:150].replace("\n", " ")))
        return
    failed.append((name, "expected a refusal mentioning %r, call succeeded" % needle))
    print("FAIL %s :: expected refusal mentioning %r" % (name, needle))


def eq(got, want, what):
    if got != want:
        raise AssertionError("%s: got %r want %r" % (what, got, want))
    return "%s=%r" % (what, got)


@contextmanager
def chunk_size(nbytes):
    previous = options.resource_chunk_size
    options.resource_chunk_size = nbytes
    try:
        yield
    finally:
        options.resource_chunk_size = previous


def raw(path, method="GET", body=None, headers=None):
    request = urllib.request.Request(ENDPOINT + path, data=body, method=method, headers=headers or {})
    with urllib.request.urlopen(request) as response:
        return response.read(), {k.lower(): v for k, v in response.headers.items()}


def raw_error(path, method="GET", body=None, headers=None):
    try:
        raw(path, method=method, body=body, headers=headers)
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode(), {k.lower(): v for k, v in error.headers.items()}
    raise AssertionError("expected an HTTP error from %s %s" % (method, path))


def names_in(text):
    return re.findall(r"<Name>([^<]+)</Name>", text)


def marker_in(text):
    found = re.search(r"<Marker>([^<]*)</Marker>", text)
    return found.group(1) if found else ""


odps = ODPS("test-ak", "test-sk", project=PROJECT, endpoint=ENDPOINT)
BLOB = bytes(bytearray(range(256))) * 20  # 5120 bytes
TEXT = "".join("row %05d\n" % i for i in range(400)).encode()  # 4400 bytes


def quiet(fn, *args, **kw):
    try:
        return fn(*args, **kw)
    except Exception:
        return None


def cleanup():
    # A part left behind by an aborted run of *this* probe would otherwise be
    # attributed to the next one. Sweep them by our own prefix, so the cleanup
    # can never touch another client's in-flight upload.
    try:
        stale = [r.name for r in odps.list_resources(prefix=PREFIX) if ".part.tmp." in r.name]
    except Exception:
        stale = []
    for stale_part in stale:
        quiet(odps.delete_resource, stale_part)
    for suffix in ("single.py", "bytes.bin", "sio.txt", "chunk.bin", "upd.bin", "schema.bin",
                   "big.bin", "local.txt", "streamres.bin", "onlyschema.bin", "malformed.bin", "malformed2.bin",
                   "badmd5.bin", "paged_%02d.bin"):
        for schema in (None, SCHEMA):
            kw = {"schema": schema} if schema else {}
            try:
                odps.delete_resource(PREFIX + "_" + suffix % (0,) if "%" in suffix else PREFIX + "_" + suffix, **kw)
            except Exception:
                pass
    for fn in ("udf", "cross", "schema"):
        for schema in (None, SCHEMA):
            kw = {"schema": schema} if schema else {}
            try:
                odps.delete_function(PREFIX + "_" + fn, **kw)
            except Exception:
                pass


cleanup()

# --- 1. single payload, the shape the Java SDK never sends -------------------
case("create py resource from str", lambda: (
    odps.create_resource(PREFIX + "_single.py", "py", fileobj="print('ok')\n"),
    eq(odps.get_resource(PREFIX + "_single.py").open("rb").read(), b"print('ok')\n", "roundtrip"),
)[-1])
case("create file resource from bytes", lambda: (
    odps.create_resource(PREFIX + "_bytes.bin", "file", fileobj=BLOB),
    eq(odps.get_resource(PREFIX + "_bytes.bin").size, len(BLOB), "meta size"),
)[-1])
case("create file resource from BytesIO", lambda: (
    odps.create_resource(PREFIX + "_sio.txt", "file", fileobj=io.BytesIO(TEXT)),
    eq(odps.get_resource(PREFIX + "_sio.txt").open("rb").read(), TEXT, "bytes roundtrip"),
)[-1])
def meta_headers():
    # Headers only: PyODPS reads metadata from the meta response headers, and a
    # create() in the same process hands back a cached object whose header-derived
    # fields are only populated after a reload (see the record for the caveat).
    text, headers = raw("/projects/%s/resources/%s_chunk.bin?meta&curr_project=%s" % (PROJECT, PREFIX, PROJECT))
    got = (headers.get("x-odps-resource-type"), headers.get("x-odps-resource-istemp"),
           headers.get("x-odps-comment"), headers.get("content-md5"),
           int(headers.get("x-odps-resource-size", -1)),
           bool(headers.get("x-odps-creation-time")), bool(headers.get("last-modified")))
    return eq(got, ("FILE", "false", "probe comment", hashlib.md5(BLOB).hexdigest(), len(BLOB), True, True),
              "meta headers")


case("meta headers carry type / temp / comment / md5 / size / times", lambda: (
    odps.create_resource(PREFIX + "_chunk.bin", "file", fileobj=BLOB, comment="probe comment"),
    meta_headers(),
)[-1])
case("downloaded payload matches the advertised MD5", lambda: eq(
    hashlib.md5(odps.get_resource(PREFIX + "_chunk.bin").open("rb").read()).hexdigest(),
    hashlib.md5(BLOB).hexdigest(), "payload md5"))


case("temp resource keeps its flag", lambda: (
    odps.create_resource(PREFIX + "_upd.bin", "file", fileobj=b"t", temp=True),
    eq(odps.get_resource(PREFIX + "_upd.bin").is_temp_resource, True, "is_temp"),
)[-1])

# --- 2. PyODPS chunked upload: its own part names, then merge -----------------
def stream_write():
    resource = odps.create_resource(PREFIX + "_big.bin", "file", fileobj=b"placeholder")
    with chunk_size(CHUNK):
        with resource.open("wb", stream=True) as fp:
            for offset in range(0, len(BLOB), 512):
                fp.write(BLOB[offset:offset + 512])
    fresh = odps.get_resource(PREFIX + "_big.bin")
    eq(fresh.size, len(BLOB), "merged size")
    eq(fresh.open("rb").read(), BLOB, "merged payload")
    eq(fresh.content_md5, hashlib.md5(BLOB).hexdigest(), "merged md5")
    # Scope the sweep to this case's own resource: a temp part left behind by a
    # different client (a refused duplicate create keeps the chunks it uploaded,
    # which is the service behaviour the probe pins further down) is not a
    # regression in *our* merge, and asserting project-wide made this case depend
    # on what else had run against the instance before it.
    leftovers = [r.name for r in odps.list_resources(prefix=PREFIX + "_big.bin") if ".part.tmp." in r.name]
    eq(leftovers, [], "this resource's temp parts removed after merge")
    return "size=%d md5=verified parts_left=%d" % (fresh.size, len(leftovers))


case("stream write (part+merge) lands byte-exact", stream_write)


def upload_with_stream():
    # create_resource(fileobj=BytesIO(big)) with a small chunk size goes through
    # PyODPS' _upload_with_stream -> part uploads -> merge.
    name = PREFIX + "_streamres.bin"
    try:
        odps.delete_resource(name)
    except Exception:
        pass
    with chunk_size(CHUNK):
        odps.create_resource(name, "file", fileobj=io.BytesIO(BLOB))
    fresh = odps.get_resource(name)
    eq(fresh.size, len(BLOB), "size after chunked upload")
    eq(fresh.open("rb").read(), BLOB, "payload after chunked upload")
    odps.delete_resource(name)
    return "size=%d" % fresh.size


case("create_resource with a small chunk size uploads parts and merges", upload_with_stream)


def local_write():
    with odps.get_resource(PREFIX + "_bytes.bin").open("wb") as fp:
        fp.write(b"abc")
        fp.write(b"defg")
    fresh = odps.get_resource(PREFIX + "_bytes.bin")
    eq(fresh.open("rb").read(), b"abcdefg", "buffered write")
    eq(fresh.size, 7, "size after buffered write")
    return "payload=abcdefg size=7"


case("non-stream open('wb') commits on close", local_write)

# --- 3. chunked reads ---------------------------------------------------------
def stream_read():
    with odps.get_resource(PREFIX + "_big.bin").open("rb", stream=True) as fp:
        with chunk_size(CHUNK):
            data = b"".join(iter(lambda: fp.read(300), b""))
    eq(data, BLOB, "streamed read")
    return "read %d bytes through rOffset/rSize" % len(data)


case("stream read is byte-exact across chunk boundaries", stream_read)


def has_remaining_flag():
    base = "/projects/%s/resources/%s_big.bin?curr_project=%s" % (PROJECT, PREFIX, PROJECT)
    first, headers = raw(base + "&rOffset=0&rSize=%d" % CHUNK)
    mid, _ = raw(base + "&rOffset=%d&rSize=%d" % (len(BLOB) - 10, 10))
    last, lh = raw(base + "&rOffset=%d&rSize=%d" % (0, len(BLOB)))
    eq(len(first), CHUNK, "first page length")
    eq(headers.get("x-odps-resource-has-remaining", "").lower(), "true", "has-remaining mid-file")
    eq(mid, BLOB[-10:], "tail page content")
    eq(lh.get("x-odps-resource-has-remaining", "").lower(), "false", "has-remaining at EOF")
    eq(int(headers.get("x-odps-resource-size", -1)), len(BLOB), "size header on download")
    return "has-remaining true/false verified"


case("rOffset/rSize reads report has-remaining honestly", has_remaining_flag)


def range_beyond_eof():
    code, body, headers = raw_error(
        "/projects/%s/resources/%s_big.bin?curr_project=%s&rOffset=%d" % (PROJECT, PREFIX, PROJECT, len(BLOB) + 10))
    if code == 200:
        raise AssertionError("offset past EOF returned 200")
    eq("x-odps-request-id" in {k.lower() for k in headers}, True, "request id present")
    return "refused with %d %s" % (code, re.search(r"<Code>([^<]+)", body).group(1) if "<Code>" in body else "")


case("a read offset past the end is refused, not served", range_beyond_eof)

# --- 4. update / overwrite ----------------------------------------------------
def update_payload():
    odps.get_resource(PREFIX + "_sio.txt").update(file_obj=io.BytesIO(b"shorter now"))
    fresh = odps.get_resource(PREFIX + "_sio.txt")
    eq(fresh.open("rb").read(), b"shorter now", "payload after update")
    eq(fresh.size, len(b"shorter now"), "size after update")
    return "payload updated"


case("update(file_obj) replaces the payload", update_payload)


def duplicate_is_refused():
    try:
        odps.create_resource(PREFIX + "_bytes.bin", "file", fileobj=b"again")
    except Exception as exc:
        eq("ResourceAlreadyExists" in str(exc), True, "code")
        return "ResourceAlreadyExists"
    raise AssertionError("duplicate create succeeded")


case("a duplicate single-payload create is refused", duplicate_is_refused)

# --- 5. listing ---------------------------------------------------------------
case("list_resources(prefix=) filters", lambda: eq(
    sorted(r.name for r in odps.list_resources(prefix=PREFIX + "_single")), [PREFIX + "_single.py"], "prefix listing"))


def type_filter():
    text, _ = raw("/projects/%s/resources?type=PY&expectmarker=true&curr_project=%s" % (PROJECT, PROJECT))
    got = [n for n in names_in(text.decode()) if n.startswith(PREFIX)]
    return eq(got, [PREFIX + "_single.py"], "type filter")


case("type filter narrows the listing", type_filter)


def paginate():
    for index in range(6):
        odps.create_resource(PREFIX + "_paged_%02d.bin" % index, "file", fileobj=b"p")
    seen, marker, pages = [], "", 0
    while True:
        text, _ = raw("/projects/%s/resources?maxitems=2&marker=%s&expectmarker=true&curr_project=%s"
                      % (PROJECT, urllib.parse.quote(marker), PROJECT))
        page = names_in(text.decode())
        overlap = set(seen) & set(page)
        if overlap:
            raise AssertionError("pages overlap on %s" % sorted(overlap))
        if sorted(page) != page:
            raise AssertionError("page not ordered: %s" % page)
        seen += page
        marker = marker_in(text.decode())
        pages += 1
        if not marker or pages > 50:
            break
    via_sdk = sorted(r.name for r in odps.list_resources())
    eq(sorted(seen), via_sdk, "paged walk == full listing")
    eq(pages >= 4, True, "pagination actually spanned pages (got %d)" % pages)
    for index in range(6):
        odps.delete_resource(PREFIX + "_paged_%02d.bin" % index)
    return "pages=%d resources=%d" % (pages, len(seen))


case("marker pagination is ordered, disjoint, complete", paginate)

# --- 6. schema scoping --------------------------------------------------------
def schema_scoped():
    odps.create_resource(PREFIX + "_schema.bin", "file", fileobj=b"in-schema", schema=SCHEMA)
    eq(odps.exist_resource(PREFIX + "_schema.bin"), False, "default schema must not see it")
    eq(odps.get_resource(PREFIX + "_schema.bin", schema=SCHEMA).open("rb").read(), b"in-schema", "scoped payload")
    odps.create_resource(PREFIX + "_schema.bin", "file", fileobj=b"in-default")
    eq((odps.get_resource(PREFIX + "_schema.bin").open("rb").read(),
        odps.get_resource(PREFIX + "_schema.bin", schema=SCHEMA).open("rb").read()),
       (b"in-default", b"in-schema"), "same name in two schemas")
    eq([r.name for r in odps.list_resources(schema=SCHEMA)], [PREFIX + "_schema.bin"], "scoped listing")
    return "isolated and independent"


case("resources are scoped by curr_schema", schema_scoped)


def function_schema_ref():
    # the resource exists only in SCHEMA, so a default-schema function must not see it
    odps.create_resource(PREFIX + "_onlyschema.bin", "file", fileobj=b"schema only", schema=SCHEMA)
    eq(odps.exist_resource(PREFIX + "_onlyschema.bin"), False, "control: absent from the default schema")
    try:
        odps.create_function(PREFIX + "_cross", class_type="com.example.Cross",
                             resources=[PREFIX + "_onlyschema.bin"])
    except Exception as exc:
        eq("unavailable resource" in str(exc), True, "refusal reason")
        quiet(odps.delete_function, PREFIX + "_cross", schema=SCHEMA)
        quiet(odps.delete_resource, PREFIX + "_onlyschema.bin", schema=SCHEMA)
        return "refused across schemas"
    quiet(odps.delete_function, PREFIX + "_cross")
    quiet(odps.delete_resource, PREFIX + "_onlyschema.bin", schema=SCHEMA)
    raise AssertionError("a default-schema function borrowed a resource from another schema")


case("a function cannot borrow a resource from another schema", function_schema_ref)

# --- 7. merge requests a client must never be rewarded for --------------------
def merge_with_no_parts():
    # What PyODPS 0.13.2 sends when a bytes payload is larger than
    # options.resource_chunk_size: it never creates a part, so the merge
    # manifest is empty. A lenient 200 here would publish a truncated resource.
    headers = {"Content-Type": "application/octet-stream",
               "x-odps-resource-type": "file",
               "x-odps-resource-name": PREFIX + "_malformed.bin",
               "x-odps-resource-merge-total-bytes": "0"}
    code, body, _ = raw_error("/projects/%s/resources?rOpMerge&curr_project=%s" % (PROJECT, PROJECT),
                              method="POST", body=(hashlib.md5(b"").hexdigest() + "|").encode(), headers=headers)
    eq(code, 400, "status")
    eq("InvalidParameter" in body, True, "code")
    eq(odps.exist_resource(PREFIX + "_malformed.bin"), False, "nothing was published")
    return "400 InvalidParameter, resource absent"


case("a merge that lists no parts is refused", merge_with_no_parts)


def merge_with_broken_manifest():
    headers = {"Content-Type": "application/octet-stream",
               "x-odps-resource-type": "file",
               "x-odps-resource-name": PREFIX + "_malformed2.bin",
               "x-odps-resource-merge-total-bytes": "7"}
    code, body, _ = raw_error("/projects/%s/resources?rOpMerge&curr_project=%s" % (PROJECT, PROJECT),
                              method="POST", body=b"abcdefg", headers=headers)
    eq(code, 400, "status")
    eq(odps.exist_resource(PREFIX + "_malformed2.bin"), False, "nothing was published")
    return "400 on a merge body that is not <md5>|<parts>"


case("a merge body that is not a manifest is refused", merge_with_broken_manifest)


def merge_with_wrong_md5():
    name = PREFIX + "_badmd5.bin"
    odps.create_resource(name, "file", fileobj=b"seed")
    part_headers = {"Content-Type": "application/octet-stream", "x-odps-resource-type": "file",
                    "x-odps-resource-name": name + ".part.tmp.000001.000000", "x-odps-resource-istemp": "true"}
    raw("/projects/%s/resources?rIsPart&curr_project=%s" % (PROJECT, PROJECT), method="POST",
        body=BLOB[:CHUNK], headers=part_headers)
    manifest = (hashlib.md5(BLOB[:CHUNK][::-1]).hexdigest() + "|" + name + ".part.tmp.000001.000000").encode()
    merge_headers = {"Content-Type": "application/octet-stream", "x-odps-resource-type": "file",
                     "x-odps-resource-name": name, "x-odps-resource-merge-total-bytes": str(len(BLOB[:CHUNK]))}
    code, body, _ = raw_error("/projects/%s/resources?rOpMerge&curr_project=%s" % (PROJECT, PROJECT),
                              method="POST", body=manifest, headers=merge_headers)
    eq(code, 400, "status")
    eq("MD5" in body, True, "reason mentions MD5")
    eq(odps.get_resource(name).open("rb").read(), b"seed", "previous payload survives")
    # The merge got as far as assembling the payload and then rejected it, so the
    # chunk it consumed is gone with it; only refusals decided before any part was
    # read (a duplicate create, a malformed manifest) keep the parts.
    eq([r.name for r in odps.list_resources(prefix=name) if ".part.tmp." in r.name], [],
       "part consumed by the refused merge")
    odps.delete_resource(name)
    return "400 on MD5 mismatch, old payload intact, part consumed"


case("a merge whose MD5 does not match is refused", merge_with_wrong_md5)

# --- 8. functions -------------------------------------------------------------
case("create function from resource names and objects", lambda: (
    odps.create_function(PREFIX + "_udf", class_type="com.example.ProbeUdf",
                         resources=[PREFIX + "_single.py", odps.get_resource(PREFIX + "_big.bin")]),
    eq((odps.get_function(PREFIX + "_udf").class_type,
        sorted(r.name for r in odps.get_function(PREFIX + "_udf").resources)),
       ("com.example.ProbeUdf", sorted([PREFIX + "_single.py", PREFIX + "_big.bin"])), "function meta"),
)[-1])
expect_refusal("a function referencing a missing resource is refused",
               lambda: odps.create_function(PREFIX + "_missing_ref", class_type="com.example.Nope",
                                            resources=["definitely_absent_res.py"]),
               "unavailable resource")
expect_refusal("a duplicate function create is refused",
               lambda: odps.create_function(PREFIX + "_udf", class_type="com.example.ProbeUdf",
                                            resources=[PREFIX + "_single.py"]),
               "FunctionAlreadyExists")
case("functions list is scoped by schema", lambda: (
    odps.create_function(PREFIX + "_schema", class_type="com.example.Scoped", resources=[PREFIX + "_schema.bin"],
                         schema=SCHEMA),
    eq([f.name for f in odps.list_functions(schema=SCHEMA)], [PREFIX + "_schema"], "schema listing"),
    eq(odps.exist_function(PREFIX + "_schema"), False, "not visible in default schema"),
)[-1])
case("function exists() is false after delete", lambda: (
    odps.delete_function(PREFIX + "_udf"),
    eq(odps.exist_function(PREFIX + "_udf"), False, "exists after delete"),
)[-1])

# --- 9. error contract the SDKs parse ----------------------------------------
def missing_resource_meta():
    code, body, headers = raw_error("/projects/%s/resources/%s_absent?meta&curr_project=%s" % (PROJECT, PREFIX, PROJECT))
    eq(code, 404, "status")
    eq("NoSuchObject" in body, True, "code")
    if not headers.get("x-odps-request-id"):
        raise AssertionError("missing x-odps-request-id")
    return "404 NoSuchObject request_id=...%s" % headers["x-odps-request-id"][:8]


case("a missing resource meta answers 404 NoSuchObject", missing_resource_meta)


def missing_function():
    code, body, _ = raw_error("/projects/%s/registration/functions/%s_absent?curr_project=%s" % (PROJECT, PREFIX, PROJECT))
    eq(code, 404, "status")
    eq("NoSuchObject" in body, True, "code")
    return "404 NoSuchObject"


case("a missing function answers 404 NoSuchObject", missing_function)


def exists_relies_on_404():
    odps.create_resource(PREFIX + "_local.txt", "file", fileobj=b"here")
    got = (odps.exist_resource(PREFIX + "_local.txt"), odps.exist_resource(PREFIX + "_absent"))
    eq(got, (True, False), "exists() pair")
    return "True/False"


case("exists() distinguishes present from absent", exists_relies_on_404)

cleanup()
print("---- pyodps contract probe: %d passed, %d failed" % (len(passed), len(failed)))
for name, detail in failed:
    print("FAILED %s :: %s" % (name, detail[:220].replace("\n", " ")))
sys.exit(1 if failed else 0)
