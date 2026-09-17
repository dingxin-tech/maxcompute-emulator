# 数据传输与 SDK 验收

1.0.0 的范围是表与 SQL 实例的数据传输。生产鉴权、云端存储调度、Volume、Blob 和 CDC 不在此版本中；不支持的 Action/增量或过滤读取返回错误。

## 支持矩阵

| API | 编码/模式 | 已实现语义 |
| --- | --- | --- |
| TableTunnel Download | Protobuf、Arrow | 快照、恢复、范围、列裁剪、CRC、压缩、完成 |
| TableTunnel Upload | Protobuf、Arrow | block 写入、恢复 block 列表、原子 commit、重复 commit、空上传 |
| StreamUploadSession | Protobuf pack | flush 即可见、动态/静态分区、retry trace-id 去重 |
| UpsertSession | Protobuf U/D | PRIMARY KEY、按键替换/删除、部分列更新、提交/中止、重试去重 |
| InstanceTunnel | Protobuf；HTTP Arrow 格式复用 | SQL 结果快照；Java InstanceTunnel 的 RecordReader 验收 |
| Storage 表读取 | Arrow IPC | read session/get、Index 或 Offset/Count、MaxBatchRows、投影、分区列表、preview |
| Storage 实例读取 | Arrow IPC | create/get session、Offset/Count 结果读取 |
| Storage Batch | Arrow IPC | stream 创建/恢复/version、flush 暂存、close、选择 streams commit、abort、静态分区 overwrite |
| Storage BatchCompatible | Arrow IPC | block/attempt、配额/路由令牌、CommitMessage、选择块提交 |
| Storage Streaming / StreamingRealtime | Arrow IPC | default 或分区 session、flush 即可见、Exactly-Once RowOffset |
| Storage 主键表 | Arrow IPC + `__operation` | U 替换、D 删除；批量事务提交 |

Java SDK 的 Tunnel 流式 pack 和 Upsert 入口使用 Protobuf；Arrow 流式写通过 Storage API。这里的 streaming download 是 SDK 流式消费一个有限快照，不是持续订阅新增数据。

批量暂存/快照/重试历史仅在进程生命周期内有效。DuckDB 数据文件可持久化已提交数据。多流提交在本机事务中完成，不模拟服务端的异步调度、bucket 分布或跨节点恢复。Storage Index 分片目前只返回一个逻辑 split；RowOffset 分片由 SDK 按总行数切分。动态分区 overwrite/upsert 要求完整静态分区，避免误覆盖其他分区。

## 使用示例

启动和镜像加载见 [README](../README.md)。同一 endpoint 同时用于 ODPS、Tunnel 和 Storage。

批量上传：

```java
TableTunnel tunnel = new TableTunnel(odps);
tunnel.setEndpoint(endpoint);
var upload = tunnel.createUploadSession("test_project", "sample");
try (var writer = upload.openRecordWriter(0)) {
    var row = upload.newRecord();
    row.setBigint(0, 1L);
    row.setString(1, "hello");
    writer.write(row);
}
upload.commit(new Long[]{0L});
```

Storage 批量写入及读取：

```java
try (var client = MaxStorageClient.builder()
        .endpoint(endpoint).tunnelEndpoint(endpoint).project("test_project")
        .credentialsProvider(new StaticCredentialProvider(odps.getAccount().getCredentials()))
        .build()) {
    var table = TableIdentifier.of("test_project", "sample");
    var session = client.createTableWriteSessionBuilder(table).build();
    try (var writer = session.createWriterBuilder("stream-1", 1)
            .withExactlyOnceMode(true).build().getAsRecordWriter(1024)) {
        var row = writer.newRecord(true);
        row.setBigint(0, 2L);
        row.setString(1, "storage");
        writer.write(row);
    }
    session.commit();
    var read = client.createTableReadSessionBuilder(table).build();
    for (var split : read.getSplits()) {
        try (var reader = read.createReaderBuilder(split).build().getAsRecordReader()) {
            Record row;
            while ((row = reader.read()) != null) System.out.println(row.getString(1));
        }
    }
}
```

依赖 `odps-sdk-core` 和 `odps-sdk-storage-api` 均为 `0.61.2-public`；完整 imports、生命周期和错误验证以 [EmulatorTest.java](../tests/java/src/test/java/EmulatorTest.java) 为准。改成 `.withWriteMode(WriteMode.STREAMING)` 或 `STREAMING_REALTIME` 时无需 commit，flush 后可读。

## 回归来源与边界

测试场景对照 Java SDK tunnel-test 的 UploadTest、BufferedWriterTest、StreamUploadTest、UpsertTest、ArrowUploadTest、InstanceTunnelTest，以及 Storage WriteAppendTableTest、StreamingWriteTest、WriteExactlyOnceTest、WriteDeltaTableTest、ReadAppendTableTest、TablePreviewTest、ReadEmptyTableTest、ReadInstanceTest。产品中的测试是独立编写的最小可重放验收，不是宣称整套云端 tunnel-test 原样通过；不复制内部源码或测试文件。

Go HTTP 测试进一步覆盖损坏 CRC、截断 payload、错 schema、跨表会话、重试内容冲突、offset 跳跃、未关闭 stream 提交、提交后写入，以及复杂类型 NULL/二进制和失败事务回滚。`go test -race ./...` 检查并发访问。Testcontainers 使用真正的 Linux amd64 Docker 镜像运行同一组 Java SDK 用例。

静态检查：`go vet -unreachable=false ./...`；ANTLR 4.13.2 生成的 parser 含工具固定生成的不可达语句，默认 vet 会报告这些存量警告，未手工修改生成代码。

1.1.0 的本地 strict 鉴权、quota 和故障控制见 [reliability.md](reliability.md)。
