# syntax=docker/dockerfile:1
FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171 AS runtime
LABEL org.opencontainers.image.version="1.0.0"
# The pinned bookworm base already provides libstdc++6 and libgcc_s.
RUN test -e /usr/lib/$(uname -m)-linux-gnu/libstdc++.so.6 \
    && mkdir /data && chown 65532:65532 /data
COPY LICENSE NOTICE docs/third-party-licenses.txt /usr/share/licenses/emulator/
COPY examples/seed.sql /opt/emulator/seed.sql
USER 65532:65532
WORKDIR /data
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/emulator"]
CMD ["--listen","0.0.0.0:8080"]

# For a separately verified Linux binary (e.g. the macOS arm64 Zig build).
FROM runtime AS prebuilt
COPY build/emulator-linux-amd64 /usr/local/bin/emulator

FROM golang:1.27.1-bookworm AS build
WORKDIR /src
ARG GOPROXY=https://proxy.golang.org,direct
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /emulator ./cmd/emulator

FROM runtime AS release
COPY --from=build /emulator /usr/local/bin/emulator
