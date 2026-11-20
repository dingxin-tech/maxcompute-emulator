#!/usr/bin/env bash
set -euo pipefail

fixture="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
image="${1:?usage: bash tests/go/verify.sh IMAGE [LOCAL_GO_SDK_CHECKOUT]}"
toolchain="${GO_CONSUMER_IMAGE:-golang:1.26.11-bookworm}"
docker_args=(--mount "type=bind,source=$fixture,target=/fixture,readonly")
if [[ $# -gt 1 ]]; then
  sdk="$(cd "$2" && pwd)"
  docker_args+=(--mount "type=bind,source=$sdk,target=/candidate,readonly")
fi

container="$(docker run --rm --detach \
  --mount "type=bind,source=$fixture/auth.json,target=/fixture-auth.json,readonly" \
  "$image" --listen 0.0.0.0:8080 --auth-mode strict --auth-config /fixture-auth.json --quotas go-batch)"
trap 'docker stop "$container" >/dev/null' EXIT

docker run --rm --network "container:$container" \
  "${docker_args[@]}" "$toolchain" bash -euc '
    export GOTOOLCHAIN=local GOWORK=off
    cp -a /fixture /consumer
    cd /consumer
    go list -m github.com/aliyun/aliyun-odps-go-sdk
    go test -mod=readonly -count=1 -timeout=90s -v ./...
    if [[ -d /candidate ]]; then
      go mod edit -replace github.com/aliyun/aliyun-odps-go-sdk=/candidate
      go list -m github.com/aliyun/aliyun-odps-go-sdk
      go test -mod=mod -count=1 -timeout=90s -v ./...
    fi
  '
