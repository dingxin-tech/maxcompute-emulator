# 构建

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
  -t maxcompute-emulator:1.0.0-rc.1 .
mvn -B -f tests/java/pom.xml test
docker save maxcompute-emulator:1.0.0-rc.1 | gzip > maxcompute-emulator-1.0.0-rc.1-linux-amd64.tar.gz
shasum -a 256 maxcompute-emulator-1.0.0-rc.1-linux-amd64.tar.gz
```

交付使用 prebuilt 路径，编译后的 ELF 在 Linux amd64 容器内执行并接受 Java SDK 验收。Dockerfile 的常规 release 路径供 Linux/CI 直接源码构建，发布方应在自己的 CI 重跑。不包含镜像 registry 推送步骤。

## 依赖下载缓存

源码 Docker 构建支持 `--build-arg GOPROXY=<可访问的 Go module proxy>`，默认仍为 `https://proxy.golang.org,direct`。依赖版本与完整性由 go.mod/go.sum 固定。首次下载 DuckDB 平台库较大；可使用可信的本地缓存代理，构建结果仍需执行 SDK 容器测试。
