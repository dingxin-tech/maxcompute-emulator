# Tunnel 协议与实现边界

## 当前端点

路径可选前缀 /api；schema 可使用路径 /projects/p/schemas/s/tables/t，或 curr_schema，默认 default。

| 接口 | 响应 |
| --- | --- |
| GET /projects/p/tunnel?service | 纯文本 host:port，CPP 客户端自行加 http:// |
| POST /projects/p/tables/t?downloads | JSON DownloadID/RecordCount/Status/Owner/Initiated/Schema/QuotaName，立即 normal |
| GET /projects/p/tables/t?downloadid=id | 同创建响应；绑定 project/schema/table/partition |
| GET 同路径 &data&rowrange=(start,count) | Protobuf，支持 columns 重排、范围截取 |
| GET 同路径再加 &arrow | 单个 Arrow RecordBatch，支持 raw_size 软限额 |
| POST /projects/p/tables/t?downloadid=id | 200，释放快照；再次读取 404 NoSuchDownload |
| POST /projects/p/instances | Java SDK XML SQLTask；201 + Location；执行同步完成 |
| GET /projects/p/instances/id（taskstatus/result/source） | Java SDK XML 状态/结果，失败 Task 状态 Failed |
| GET /projects/p、GET /projects/p/tables[/t] | Java SDK XML，Table.Schema 内嵌 JSON |
| GET /readyz、/healthz、/capabilities | 就绪/版本/支持范围 JSON |

HTTP 分区参数遵循 SDK 的 `ds=2026-09-14` 写法，也接受单引号值；SQL 中使用 `PARTITION(ds='2026-09-14')`。当前逗号是分区键分隔符，不支持分区值本身包含逗号。quotaName 回显而不执行配额调度，asyncmode 接受后同步建立快照。Protobuf 的 raw_size 参数不裁剪行数；当前 CPP 只在 Arrow 路径发送它。

HTTP 4xx/5xx 按端点返回 JSON 或 XML Code/Message/RequestId；NoSuchTable/NoSuchDownload、InvalidPartition/InvalidParameter/InvalidColumn、InvalidCompression、UnsupportedOperation、ResourceLimit。签名不校验。超过并发入口容量返回 503 + Retry-After。

## 编码

Protobuf 按 public SDK 的列序编码，NULL 不写字段。记录结束 tag 33553408 后写 CRC32C，流尾 meta-count 33554430 与 checksum-of-checksums 33554431；CRC 数值按小端逻辑值更新，不包含传输长度和 NULL 标记。DATE 为天数，DATETIME 为毫秒，TIMESTAMP 为秒+纳秒，DECIMAL 为十进制文本。MAP 在引擎边界从 DuckDB OrderedMap 转换，ARRAY/STRUCT 递归编码。

Tunnel Arrow 不是普通带 Schema 消息的 IPC stream：客户端从会话元数据构造 schema，服务端只发送 RecordBatch payload。帧头为大端 chunk size（65536），每满块追加 CRC32C，最后追加全流 CRC32C。压缩包裹整个 Tunnel 帧。空表返回空数据帧。Arrow 单响应最多一批，CPP BufferArrowRecordReader 按实际行数推进；raw_size 过小时仍返回至少一行，以保证进度。

## 公开源码依据

- [Java SDK](https://github.com/aliyun/aliyun-odps-java-sdk/tree/1c11317c18ad63ac84030867f5a8dd2def48122f)：TableTunnel、RawTunnelRecordReader、ArrowTunnelRecordReader、ArrowHttpInputStream、Instance。运行验收使用公开 Maven 制品 0.61.2-public。
- [Go SDK](https://github.com/aliyun/aliyun-odps-go-sdk/tree/3e824d4bced7b5bde34c96a3389f69c29d32c3f5)：record_protoc_writer、checksum 与会话模型，交叉核对线格式。
- [CPP SDK](https://github.com/aliyun/aliyun-odps-sdk-cpp/tree/a1a7e541753f53970131e345a4f77581a8d6a9ac)：tunnel/download.h、download.cpp、arrow_record_reader.cpp、arrow_http_stream.h、serialize.cpp；与当前 CK submodule 对齐。

产品线协议实现为独立 Go 实现。内部服务端源码只用作行为对照，没有复制内部代码、生成文件或测试数据。

## 迁移

1.0.0-rc.1 在 CK 下载基础上增加 Tunnel 上传、Upsert、实例下载和 Storage API 读写，详情见 [数据传输](data-transfer.md)。Storage v1、Volume/Blob 等仍需独立里程碑。只有旧功能对齐和消费者回归完成才将 v2 替换 master。现在可通过切换容器镜像回到 v1；v1 SQLite 与 v2 DuckDB 文件不兼容，用 SQL fixture 重建数据，勿挂载同一数据库文件。


## 1.0.0-rc.1 写入与 Storage 入口

| 入口 | 行为 |
| --- | --- |
| POST/GET/PUT/POST `/projects/p/tables/t?uploads` / `?uploadid=id` | 创建/恢复/写 block/提交；提交前不可见，重复提交幂等 |
| POST/GET/PUT `/projects/p/tables/t/streams` | 创建/恢复/写 pack；flush 后可见；dynamic_partition 支持逐 pack 分区 |
| POST/GET/PUT/POST/DELETE `/projects/p/tables/t/upserts` | 创建/恢复/暂存 U/D/提交/中止；支持部分列更新 |
| POST/GET `/projects/p/instances/i?downloads` / `?downloadid=id` | SQL 实例结果快照与 Tunnel 下载，复用表读取格式 |
| POST `/api/storage/v2` 或 `/api/storage/v3` | Action + Target 协议；Java 0.61.2-public 实际调用 v3 |

Storage 表 Action：TableCreateReadSession、TableGetReadSession、TableRead、TablePreview、TableCreateWriteSession、TableGetWriteSession、TableCreateWriteStream、TableGetWriteStream、TableWrite、TableCloseWriteStream、TableCommitWriteSession、TableAbortWriteSession。实例 Action：InstanceCreateReadSession、InstanceGetReadSession（GET）、InstanceRead。

Table Target 格式 `projects.p.schemas.s.tables.t`；Instance Target `projects.p.instances.i`。读 schema 为 DataSchema 平铺列；写 schema 为 TableSchema.columnType 递归类型码。Storage STRING 是 UTF8 Arrow，Tunnel STRING 是 Binary Arrow。Storage 返回含 schema/EOS 的标准 IPC stream，Tunnel 使用无 schema 的 CRC 帧。

Tunnel 写入解码完整校验后才暂存或发布；MAP 校验保留发送顺序。数据写入通过 DuckDB 事务，复杂值递归生成绑定表达式，避免驱动对 NULL 数组直接绑定的限制。所有用户数据仍走参数绑定。

Stream/Upsert 使用 SDK retry trace-id 识别同次重试，内容不一致返回409。Storage ExactlyOnceMode 校验 RowOffset 连续性，并校验同 offset 重放的内容，返回 ExactlyOnceRowOffset。BatchCompatible 返回带 BlockNumber/AttemptNumber/WriterStats 的 JSON CommitMessage，只接受本会话最新 attempt 的令牌；批量 commit 原子发布。
