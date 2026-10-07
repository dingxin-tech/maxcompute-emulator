# MaxCompute Emulator

Go + DuckDB 实现的本地 MaxCompute 测试服务。**1.1.0** 支持 ClickHouse 当前 ODPS-CPP Tunnel 读表，以及 Java SDK 的 Tunnel 批量/流式上传、Upsert、实例结果下载、Storage API 批量/流式 Arrow 读写。[English](README.md) · [发布评审与元数据/Flink 支持范围](docs/release-readiness.md)。支持矩阵和回归用例见 [docs/data-transfer.md](docs/data-transfer.md)。

`main` 为 Go 实现，原 Spring Boot/SQLite 版本保留在 [`legacy`](https://github.com/dingxin-tech/maxcompute-emulator/tree/legacy)。Storage v1 和分布式事务尚未实现。默认接受测试 AK/SK；可选 strict 模式检查本地测试凭据的签名、STS、日期和读写权限，适用于本地与 CI 测试环境。

## 启动镜像

交付包中的镜像为 Linux amd64；Apple Silicon 通过 Docker 的 amd64 模拟运行。

```bash
docker pull maxcompute/maxcompute-emulator:1.1.0
docker run --rm --name mc-emulator --platform linux/amd64 \
  -p 127.0.0.1:8080:8080 maxcompute/maxcompute-emulator:1.1.0 \
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
  maxcompute/maxcompute-emulator:1.1.0 \
  --listen 0.0.0.0:8080 --seed /fixtures.sql --project test_project
```

需要保存表数据时增加 `--database /data/emulator.duckdb`，并将可由 UID 65532 写入的目录或 volume 挂到 `/data`。会话保存在内存，重启后必须重新创建读写会话。重复执行同一 seed 前，应使用幂等 SQL 或重新创建数据目录。

## Java SDK / Testcontainers

依赖与可执行测试位于 [tests/java](tests/java)。使用 JDK 17、Maven、Docker：

```bash
mvn -B -f tests/java/pom.xml test
```

默认通过 Testcontainers 启动本地 `maxcompute/maxcompute-emulator:1.1.0`，动态分配端口，等待 `/readyz`，运行后销毁容器。可指定 `-Demulator.image=自定义镜像名:版本`；验证已启动服务用 `-Demulator.endpoint=http://127.0.0.1:8080`。

```java
try (GenericContainer<?> mc = new GenericContainer<>(
        DockerImageName.parse("maxcompute/maxcompute-emulator:1.1.0"))
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
| 类型 | BIGINT/INT/SMALLINT/TINYINT、FLOAT/DOUBLE、BOOLEAN、STRING/BINARY、DECIMAL(p,s) p≤38、DATE/DATETIME/TIMESTAMP/TIMESTAMP_NTZ、ARRAY/MAP/STRUCT |
| 分区 | SQL 和 Tunnel 下载使用完整静态分区；Tunnel 流式写支持动态分区；Storage 支持动态/静态写、跨分区快照和分区筛选 |
| 覆盖 | staging 后事务提交；失败保留原数据；已有下载会话不受后续写入影响 |
| 数据协议 | Protobuf 逐记录与全流 CRC32C；无 schema 的 Arrow RecordBatch + Tunnel chunk CRC32C |
| 压缩 | Tunnel Protobuf RAW/deflate/zstd/lz4_frame/Snappy；Arrow 增加 ZLIB/Snappy/Arrow-LZ4；Storage 标准 Arrow IPC（含 SDK 内部 ZSTD batch 压缩） |
| 时间 | 无时区值按 UTC 编码；DATETIME 毫秒、TIMESTAMP 纳秒 |
| 限制 | SQL≤1 MiB/100 语句；结果默认≤100,000 行/估算 64 MiB；每类会话默认64个、单写会话暂存≤64 MiB；16 个并发 HTTP 请求 |
| 会话 | 默认 TTL 30 分钟；完成/过期后不可恢复；仅运行期间有效 |
| Arrow 分页 | Tunnel 单响应一批；指定 raw_size 时最多65,536行且至少返回一行；不指定则返回请求范围；Storage 按 MaxBatchRows 输出多批 |
| 资源/函数 | 资源上传（单次 payload 或 Java SDK 分片+合并）、元数据读取、按 `rOffset`/`rSize` 下载、覆盖、删除、前缀与分页列举；TABLE 资源元数据；引用已存在资源的 Java/SQL/内嵌函数注册 |
| 不支持 | Storage v1、Volume/Blob、增量/CDC/过滤表达式下推、UDF 执行（函数只有元数据，SQL `CREATE FUNCTION`/`DROP FUNCTION` 与调用 UDF 返回 `UnsupportedFeature`）、Volume 资源、授权模拟、异步 SQL、完整 ODPS 函数库/隐式转换语义 |

资源 payload 上限 64 MiB/个、512 MiB/project+schema；资源名与函数名按大小写不敏感解析，回读保留上传时的大小写。

DATETIME/TIMESTAMP 常量、STRING cast、ARRAY 构造与 NAMED_STRUCT 有显式映射；这不是通用 ODPS SQL 兼容实现。支持子集以测试为准，未实现操作返回带 request-id 的错误。与真实服务端有两格已实测的差异（分片合并被拒时的状态码/错误码、`TABLE` 资源可指向不存在的表），见[与真实服务端的已知差异](#与真实服务端的已知差异)。引擎关闭外部文件/网络访问，内部数据库命名空间与函数不可从 SQL 访问。

## 与真实服务端的已知差异

下面两格差异是**真机实测**出来的，不是推断，也刻意保持现状：这些状态码与错误码正是本仓库测试已经在断言的行为，
改动它们是契约决策，不是文档修订。决策落地之前，本节就是跟踪清单——若你的实测与某一行的读数冲突，
请带两侧读数和 request id 开 issue。

两格说的都是 `main`，已发布的镜像不在这两格里。2026-10-08 直接对 `maxcompute/maxcompute-emulator:1.1.0`（digest `sha256:57d1d25757255a1f16d0a3c34ef8b99c24bf8615e1c14c13dfd5a41026dee2d3`）复核：`GET /projects/<project>/resources` 回 `404` + `UnsupportedOperation: unsupported endpoint`，`/capabilities` 既未声明 `resources` 也未声明 `functions`；当前 PyODPS 契约探针打在该镜像上收尾是 `1 passed, 32 failed`，唯一通过的那格也是因为整个端点被拒才通过。这与 2026-09-21 打在 `1.1.0` 源码构建上的 `1 passed, 29 failed` 同形——用例数随探针增长，"能力不存在"这个答复没变。也就是说 `1.1.0` 上根本没有这两格可比的对象：要用资源面请从源码构建，或等 `1.1.0` 之后的 tag。

### 1. 被拒绝的分片合并，状态码与错误码不同

分片上传的最后一步是 `?rOpMerge=true`。Java SDK 走这条路有两种情形：内容超出 64 MiB 分块缓冲，或者流的长度无法判定
（管道、网络流）——后者不论大小都分片；PyODPS 则在超过 `options.resource_chunk_size` 时分片。实测比对了其中的两种拒绝：

| 合并被拒的原因 | 真实服务端 | 模拟器 |
| --- | --- | --- |
| 合并请求体里的 MD5 与实际拼出的内容不符 | `500 InternalServerError` — `ODPS-0421213: Save resource error - Merge part temp files failed! Message: The merged file's signature does not match!` | `400 InvalidParameter` — `merged payload does not match MD5 <md5>` |
| 目标资源名已存在 | `409 ObjectAlreadyExists` — `ODPS-0421121: The resource has already existed - <name>` | `400 ResourceAlreadyExists` |

两种拒绝的**其余部分两侧一致**，可依赖的是这些：合并已经把分片拼起来之后失败，会一并消费清单里的分片，重试必须重传；
在读到任何分片之前就定案的拒绝保留分片、不动已存在的目标；清单点名一个从未上传过的分片，两侧都是 `404 NoSuchObject`。
只有拒绝的**形态**不同。

**谁会观察到，怎么绕：**按 HTTP 状态码或客户端异常类型分支的测试与重试策略。真机上这两种拒绝分别落成服务端错误
（`InternalServerError`，通常按可重试处理）与冲突（`ObjectAlreadyExists`）；模拟器把两者都压成一个 `400`。
所以一条"500 重试、400 不重试"的本地用例验的是本模拟器的分类，不是服务端的分类。断言"合并被拒绝"以及分片与目标的状态即可——
`go test ./internal/server -run TestResourceRESTContract` 钉住的正是这两格；要断言具体状态码就按目标分别钉，并在注释里写明。

### 2. `TABLE` 资源可以指向一张不存在的表

| 场景 | 真实服务端 | 模拟器 |
| --- | --- | --- |
| 创建 `TABLE` 资源，其源表从未被创建 | `404 NoSuchObject` — `ODPS-0422111: Table not found - <project>.<table>`；资源不会建立 | `201 Created`；资源可被列举，`?meta` 能回读 `TableName` |

**谁会观察到，怎么绕：**用 table 资源（或依赖它的函数）做 fixture、而表名写错或表还没建的用例。本地通过、线上失败，
而且线上是在**创建**那一步就失败，不是等到首次使用；只在模拟器上跑过的套件会对一个服务端根本不接受的引用报绿。
模拟器要求 `x-odps-copy-table-source` 非空，也会校验函数引用的**资源**是否存在，`TABLE` 资源背后的表是唯一不解析的指针。
在 seed 里建表，或在 fixture 前置断言 `odps.exist_table(...)` / `odps.tables().exists(...)`，让坏引用在本地按同一个理由失败。

### 这些读数没覆盖什么

两格都取自**单个真实项目**，只从客户端可见层读取（PyODPS 异常对象的 `status_code`、`code` 与消息文本），
endpoint 形态是 `http://`，运行环境 Linux/amd64，时间 2026-10-02 至 2026-10-05；模拟器一列在主干 `24f3fce` 上复测。
以下按未验证陈述，不当作结论：

- 其他 region、其他服务端版本，或走带信任链的公网 HTTPS 时是否给出同样的码——这几轮没有经过 TLS 终结或网关，
  网关额外产生的错误形态一概未知；
- BSD 或 macOS 原生构建：上表的模拟器读数来自 Linux/amd64 二进制（镜像或本机构建）。Apple Silicon 以模拟方式运行同一个
  Linux 镜像，那是另一件事，这几轮没有做；
- 其他合并拒绝形态（请求体格式不合法、分片超限、配额拒绝）从未比对过，所以本表不否认第三种差异存在，它只是未测。
  有一个例外是已知的，而且方向相反：申报的 `x-odps-resource-merge-total-bytes` 与实际拼装长度不符时，
  服务端不看这个申报值（2026-10-08 复测：304 字节的合并按 4400 申报，被接受，发布内容逐字节正确），
  本模拟器会拒绝。这第三种差异写在[协议与实现边界](docs/protocol.md)里而不是本节，因为它是模拟器比服务端更严，
  不是模拟器缺少的服务端行为。

不带项目也能复测模拟器一侧：`go test ./internal/server -run TestResourceRESTContract` 钉住第 1 格的状态码与分片/目标去向；`python tests/python/run.py http://127.0.0.1:8080`（dummy 凭据、合成数据，已挂进 CI）用 PyODPS 客户端断言同样两种合并拒绝——它钉的是分片去向，刻意不钉服务端的状态数字。第 2 格本仓库没有用例：Go 用例只在**已存在**的表上注册 `TABLE` 资源，所以“表不存在仍接受创建”这条是按实测记录，不是按用例记录。

服务端一侧需要你自己的项目：上传一个分片（`project.resources.create(name=..., type="file", temp=True, part=True, fileobj=...)`）、用与实际内容不符的 MD5 调 `project.resources.merge_part_files(...)`、再对一个已存在的名字重放同一次合并，最后 `odps.create_resource(name, "table", table_name="<从未创建的表>")`，把客户端抛出的 `status_code`、`code` 与消息打印出来。同样的三个读数被两轮独立复现，第 1、2 格才能从“测过一次”升级成“钉住了”。服务端一列需要你自己的项目：
上传一个分片（`project.resources.create(name=..., type="file", temp=True, part=True, fileobj=...)`），
用与实际内容不符的 MD5 调 `project.resources.merge_part_files(...)`，再对一个已存在的名字重放同一次合并，
最后 `odps.create_resource(name, "table", table_name="<从未创建的表>")`——把客户端抛出的 `status_code`、`code`
与消息打印出来比对。

## 构建与验证

```bash
# Go 1.27.1 + C/C++ 编译器，本机运行
go test ./...
go run ./cmd/emulator --seed examples/seed.sql

# 标准 Linux Docker 源码构建（CGO 开启）
docker build --platform linux/amd64 -t maxcompute/maxcompute-emulator:1.1.0 .

# 镜像加载/构建后，真实 Java SDK 容器验收
mvn -B -f tests/java/pom.xml test
```

本次交付也验证了 macOS arm64 → Linux amd64 的 Zig 构建路径，命令见 [docs/build.md](docs/build.md)。模块依赖锁定在 go.mod/go.sum；ANTLR 生成代码已入库，日常构建无需 Java。语法来源与再生成流程见 [grammar/README.md](grammar/README.md)。

Apache-2.0；第三方来源见 [NOTICE](NOTICE)。

[可靠性测试配置](docs/reliability.md)：session JSON 日志、协议故障注入、命名 quota、可选严格鉴权与 SQL 类型 fixture。
