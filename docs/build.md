# 构建

## 从源码构建并验证候选版本

Dockerfile 的 `build` 阶段包含源码与 CGO 依赖，可用同一阶段运行完整 Go 测试。
用最终 `release` 阶段生成运行镜像，再让 Java SDK 对接候选镜像。以下以 ARM64
为例，AMD64 环境可替换平台。开发候选版本的功能不能用旧的发布镜像替代验证。

```sh
docker build --platform linux/arm64 --target build -t maxcompute-emulator:test-build .
docker run --rm --platform linux/arm64 maxcompute-emulator:test-build go test ./... -count=1
docker build --platform linux/arm64 --target release -t maxcompute-emulator:candidate .
docker image inspect maxcompute-emulator:candidate --format '{{.Id}}'
git rev-parse HEAD
mvn -B -f tests/java/pom.xml -Demulator.image=maxcompute-emulator:candidate test
```

保存源码 commit、镜像 ID/发布 digest、Go 和消费者日志。容器内 Go 测试与
Java SDK 测试是独立证据；只完成编译不能代替验收。写故障接口见
[可靠性控制](reliability.md)，Connector 使用范围见[协议与验收](connector-conformance.md)。

标准 Dockerfile 的 release 阶段在 Linux 内编译 CGO。原生 Go 构建要求 C/C++ 编译器，不能用 CGO_ENABLED=0。Docker 运行层使用 Debian bookworm/glibc 2.36 + libstdc++6，非 Alpine。

## 本次验证的交叉构建

宿主 macOS arm64，Go 1.27.1、Zig 0.15.2、Debian bookworm amd64 C++ runtime sysroot。仅设置 GOOS/GOARCH 的默认 clang 无法完成 Linux CGO；Zig 还需要与 DuckDB 静态库兼容的 GNU C++ runtime。

准备 Zig 可执行文件和包含下列文件的 amd64 bookworm rootfs：

- usr/lib/x86_64-linux-gnu/libstdc++.so.6
- lib/x86_64-linux-gnu/libgcc_s.so.1

rootfs 可从本机已拉取的 amd64 Debian bookworm 系镜像 docker create/export 提取（如安装了 libstdc++6 的运行层）。它只用于链接，不入产品源码。

```bash
ZIG=/path/to/zig DUCKDB_SYSROOT=/path/to/bookworm-amd64-rootfs \
  scripts/build-linux-amd64.sh
docker build --platform linux/amd64 --target prebuilt \
  -t maxcompute-emulator:1.1.0 .
mvn -B -f tests/java/pom.xml test
docker save maxcompute-emulator:1.1.0 | gzip > maxcompute-emulator-1.1.0-linux-amd64.tar.gz
shasum -a 256 maxcompute-emulator-1.1.0-linux-amd64.tar.gz
```

社区发布使用 Dockerfile 的 release 阶段在 Linux amd64 内从源码构建。prebuilt 是可选的本地交叉编译入口。tag 发布流程先执行 Go/SDK 验收，再推送 Docker Hub 并从该仓库拉取复测。

## 依赖下载缓存

源码 Docker 构建支持 `--build-arg GOPROXY=<可访问的 Go module proxy>`，默认仍为 `https://proxy.golang.org,direct`。依赖版本与完整性由 go.mod/go.sum 固定。首次下载 DuckDB 平台库较大；可使用可信的本地缓存代理，构建结果仍需执行 SDK 容器测试。
