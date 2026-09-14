"""Collect license text from the pinned, downloaded Go module graph."""
import json
import subprocess
import os
from pathlib import Path

raw = subprocess.check_output(["go", "list", "-deps", "-json", "./cmd/emulator"], text=True, env={**os.environ, "GOOS":"linux", "GOARCH":"amd64", "CGO_ENABLED":"1"})
decoder = json.JSONDecoder()
modules = {}
while raw.strip():
    obj, end = decoder.raw_decode(raw.lstrip())
    raw = raw.lstrip()[end:]
    module = obj.get("Module")
    if module and not module.get("Main"):
        modules[module["Path"]] = module
out = ["# Third-party Go module licenses\n\nGenerated from go.mod/go.sum by scripts/third-party-notices.py.\n"]
missing = []
for module in sorted(modules.values(), key=lambda x: x["Path"]):
    root = Path(module.get("Dir", "/nonexistent"))
    files = sorted(p for p in root.glob("*") if p.is_file() and
                   p.name.upper().startswith(("LICENSE", "COPYING", "NOTICE", "COPYRIGHT")))
    if not files:
        missing.append(module["Path"])
        continue
    out.append("\n## " + module["Path"] + " " + module["Version"] + "\n")
    for file in files:
        out.append("\n### " + file.name + "\n\n" + file.read_text(errors="replace") + "\n")
if missing:
    raise SystemExit("Missing license files; run go mod download and inspect: " + ", ".join(missing))
Path("docs/third-party-licenses.txt").write_text("".join(out))
