#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${ZIG:?Set ZIG to Zig 0.15.2 executable}"
: "${DUCKDB_SYSROOT:?Set DUCKDB_SYSROOT to a Debian bookworm amd64 rootfs with libstdc++6}"
test -f "$DUCKDB_SYSROOT/usr/lib/x86_64-linux-gnu/libstdc++.so.6"
test -f "$DUCKDB_SYSROOT/lib/x86_64-linux-gnu/libgcc_s.so.1"
mkdir -p build
export GOOS=linux GOARCH=amd64 CGO_ENABLED=1
export CC="$ZIG cc -target x86_64-linux-gnu.2.36"
export CXX="$ZIG c++ -target x86_64-linux-gnu.2.36"
export CGO_LDFLAGS="$DUCKDB_SYSROOT/usr/lib/x86_64-linux-gnu/libstdc++.so.6 $DUCKDB_SYSROOT/lib/x86_64-linux-gnu/libgcc_s.so.1"
go build -trimpath -ldflags="-s -w" -o build/emulator-linux-amd64 ./cmd/emulator
