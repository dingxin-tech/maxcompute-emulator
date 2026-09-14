# MaxCompute Emulator v2

Go + DuckDB 实现的本地 MaxCompute 测试服务。**2.1.0** 支持 ClickHouse 当前 ODPS-CPP Tunnel 读表，以及 Java SDK 的 Tunnel 批量/流式上传、Upsert、实例结果下载、Storage API 批量/流式 Arrow 读写。支持矩阵和回归用例见 [docs/data-transfer.md](docs/data-transfer.md)。

当前开发分支为 `v2`，还未替换 v1 的 `master`。权限模拟、Storage v1 和分布式事务尚未实现。本服务接受测试 AK/SK，不验证签名，适用于本地与 CI 测试环境。

## 启动镜像

交付包中的镜像为 Linux amd64；Apple Silicon 通过 Docker 的 amd64 模拟运行。

```bash
docker load -i maxcompute-emulator-2.1.0-linux-amd64.tar.gz
docker run --rm --name mc-emulator --platform linux/amd64 \
  -p 127.0.0.1:8080:8080 maxcompute-emulator:2.1.0 \
  --listen 0.0.0.0:8080 --seed /opt/emulator/seed.sql
curl -fsS http://127.0.0.1:8080/readyz
```

服务地址和 Tunnel 地址均为 `http://127.0.0.1:8080`。内置示例创建：

- 项目 `test_project`、schema `default`；
- `demo`：3 行，BIGINT/STRING/DECIMAL/BOOLEAN，包含 NULL；
- `events`：静态分区数据，分区定义见 [examples/seed.sql](examples/seed.sql)。

不传 `--seed` 则启动空服务，通过 SDK SQL 创建测试数据。项目与 schema 在首次使用时自动建立。默认数据驻留内存，容器退出后清空。

如需自己的数据文件：

```bash
docker run --rm --platform linux/amd64 -p 127.0.0.1:8080:8080 \
  -v "$PWD/fixtures.sql:/fixtures.sql:ro" \
  maxcompute-emulator:2.1.0 \
  --listen 0.0.0.0:8080 --seed /fixtures.sql --project test_project
```

需要保存表数据时增加 `--database /data/emulator.duckdb`，并将可由 UID 65532 写入的目录或 volume 挂到 `/data`。会话保存在内存，重启后必须重新创建读写会话。重复执行同一 seed 前，应使用幂等 SQL 或重新创建数据目录。

## Java SDK / Testcontainers

依赖与可执行测试位于 [tests/java](tests/java)。使用 JDK 17、Maven、Docker：

```bash
mvn -B -f tests/java/pom.xml test
```

默认通过 Testcontainers 启动本地 `maxcompute-emulator:2.1.0`，动态分配端口，等待 `/readyz`，运行后销毁容器。可指定 `-Demulator.image=内部镜像名:版本`；验证已启动服务用 `-Demulator.endpoint=http://127.0.0.1:8080`。

```java
try (GenericContainer<?> mc = new GenericContainer<>(
        DockerImageName.parse("maxcompute-emulator:2.1.0"))
        .withExposedPorts(8080)
        .waitingFor(Wait.forHttp("/readyz"))) {
    mc.start();
    String endpoint = "http://" + mc.getHost() + ":" + mc.getMappedPort(8080);
    Odps odps = new Odps(new AliyunAccount("test-ak", "test-sk"));
    odps.setEndpoint(endpoint);
    odps.setTunnelEndpoint(endpoint);
    odps.setDefaultProject("test_project");
    odps.setCurrentSchema("default");
    SQLTask.run(odps, "create table sample(id bigint, name string)").waitForSuccess();
    SQLTask.run(odps, "insert into sample values(1,'hello')").waitForSuccess();
    TableTunnel tunnel = new TableTunnel(odps);
    tunnel.setEndpoint(endpoint);
    TableTunnel.DownloadSession session =
        tunnel.createDownloadSession("test_project", "sample");
    try (TunnelRecordReader reader = session.openRecordReader(0, session.getRecordCount())) {
        System.out.println(reader.read().getString(1)); // hello
    }
}
```

测试 POM 默认使用 Docker API 1.44，兼容本次 Docker 29 环境；旧 Docker 可通过 `-Ddocker.api.version=1.40` 调整。

使用 Arrow 时，JDK 17 需要 `--add-opens=java.base/java.nio=ALL-UNNAMED`，测试 POM 已配置。Java SDK 0.61.2-public 的 Arrow Reader 不能选择 CK 使用的 zstd/lz4_frame 压缩枚举；Java 测试验证 RAW/ZLIB/SNAPPY/ARROW_LZ4_FRAME Arrow，HTTP 协议测试另行验证 CK 的 zstd/lz4_frame。

v1 Testcontainers 的模式保留，但就绪条件改为 HTTP `/readyz`，无需等待 Spring 日志。通常不再需要调用 `POST /init`：路由返回请求 Host（含映射端口）。反向代理或特殊网络可传 `--public-endpoint http://客户端可访问地址:端口`；旧 `POST /init` 设置地址方式也保留。

## ClickHouse 侧连接

将 ClickHouse 当前 ODPS 引擎的 endpoint 与 tunnel endpoint 指向本服务，project 设为 `test_project`，AK/SK 可用任意非空测试值，表名用 `demo` 或自己的 SQL fixture。两者处于同一 Docker 网络时使用容器 DNS 名，例如 `http://mc-emulator:8080`；ClickHouse 在宿主机时使用映射端口。避免将另一个容器中的 localhost 当成本服务地址。

按照当前 ODPS-CPP SDK 的调用方式，会话创建返回 `Status=normal`，随后可获取总行数、列 schema，按 `rowrange` 并行读取或按 Arrow batch 实际行数翻页，完成后 POST 关闭会话。相关接口、支持边界和源码依据见 [docs/protocol.md](docs/protocol.md)。

本版本有 Java SDK 与协议测试证据；完整 ClickHouse 分支集成测试需由对应引擎构建接入，本仓库不将其标记为已通过。

## 支持范围

| 能力 | 当前行为 |
| --- | --- |
| SQL | CREATE/DROP TABLE、INSERT INTO、INSERT OVERWRITE、SELECT/WITH、常用 DuckDB 兼容表达式；ANTLR 验证 ODPS 语法 |
| 类型 | BIGINT/INT/SMALLINT/TINYINT、FLOAT/DOUBLE、BOOLEAN、STRING/BINARY、DECIMAL(p,s) p≤38、DATE/DATETIME/TIMESTAMP、ARRAY/MAP/STRUCT |
| 分区 | SQL 和 Tunnel 下载使用完整静态分区；Tunnel 流式写支持动态分区；Storage 支持动态/静态写、跨分区快照和分区筛选 |
| 覆盖 | staging 后事务提交；失败保留原数据；已有下载会话不受后续写入影响 |
| 数据协议 | Protobuf 逐记录与全流 CRC32C；无 schema 的 Arrow RecordBatch + Tunnel chunk CRC32C |
| 压缩 | Tunnel Protobuf RAW/deflate/zstd/lz4_frame/Snappy；Arrow 增加 ZLIB/Snappy/Arrow-LZ4；Storage 标准 Arrow IPC（含 SDK 内部 ZSTD batch 压缩） |
| 时间 | 无时区值按 UTC 编码；DATETIME 毫秒、TIMESTAMP 纳秒 |
| 限制 | SQL≤1 MiB/100 语句；结果默认≤100,000 行/估算 64 MiB；每类会话默认64个、单写会话暂存≤64 MiB；16 个并发 HTTP 请求 |
| 会话 | 默认 TTL 30 分钟；完成/过期后不可恢复；仅运行期间有效 |
| Arrow 分页 | Tunnel 单响应一批；指定 raw_size 时最多65,536行且至少返回一行；不指定则返回请求范围；Storage 按 MaxBatchRows 输出多批 |
| 不支持 | Storage v1、Volume/Blob、增量/CDC/过滤表达式下推、资源/UDF、授权模拟、异步 SQL、完整 ODPS 函数库/隐式转换语义 |

DATETIME/TIMESTAMP 常量、STRING cast、ARRAY 构造与 NAMED_STRUCT 有显式映射；这不是通用 ODPS SQL 兼容实现。支持子集以测试为准，未实现操作返回带 request-id 的错误。引擎关闭外部文件/网络访问，内部数据库命名空间与函数不可从 SQL 访问。

## 构建与验证

```bash
# Go 1.27.1 + C/C++ 编译器，本机运行
go test ./...
go run ./cmd/emulator --seed examples/seed.sql

# 标准 Linux Docker 源码构建（CGO 开启）
docker build --platform linux/amd64 -t maxcompute-emulator:2.1.0 .

# 镜像加载/构建后，真实 Java SDK 容器验收
mvn -B -f tests/java/pom.xml test
```

本次交付也验证了 macOS arm64 → Linux amd64 的 Zig 构建路径，命令见 [docs/build.md](docs/build.md)。模块依赖锁定在 go.mod/go.sum；ANTLR 生成代码已入库，日常构建无需 Java。语法来源与再生成流程见 [grammar/README.md](grammar/README.md)。

Apache-2.0；第三方来源见 [NOTICE](NOTICE)。
