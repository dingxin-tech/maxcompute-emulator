# 1.0.0 发布候选与兼容范围

当前版本 `1.0.0-rc.1`。此前 2.0.0-ck.1 / 2.1.0 是 Go 重写的内部开发制品；首次社区版本从 1.0.0 开始。RC 不代表正式社区镜像已发布。v1 master 仍保留，不能将本版解释为全部 v1 功能的无损替换。

## 元数据

| 能力 | RC 行为 | 后续边界 |
| --- | --- | --- |
| 项目查询 | 测试项目按请求自动存在，回读 owner/schema 开关 | 不模拟账号权限或真实项目注册 |
| 表查询/exists/list | Java SDK XML，表 ID 持久化；name 前缀、marker/maxitems 分页 | 不含 views、外表定义、授权、标签、资源和函数管理 |
| 表 schema | 字段/分区字段/类型/nullable/comment | 暂不支持 ALTER ADD/DROP/CHANGE COLUMN |
| 表扩展属性 | PRIMARY KEY、transactional、schema version、创建时间、lifecycle 回读 | schema version 固定 1（不支持演进）；时间非云端统计；lifecycle 不自动清理 |
| PK bucket metadata | 模拟单个 HASH bucket，与本地 upsert 协议一致 | 不模拟云端分桶/分布式事务 |
| 分区 | STRING 静态/动态分区，SDK create/delete/has/get/list，前缀过滤及分页 | 非 STRING 分区拒绝；无分区权限/存储层统计 |
| 空分区 | ADD 后无数据仍存在；清空/删除最后行和重启后保留 | DROP PARTITION 才删除已登记分区 |
| SDK DDL | 同一 project/schema 的限定表名；CREATE/DROP、ADD/DROP PARTITION、TRUNCATE | 跨命名空间 SQL 不支持；显式 Schema 管理 API 尚未实现 |

## 数据面评审修复

- SQL 在执行前预留实例容量，容量满不能先提交再返回失败。
- 每个表有持久化 ID；DROP/CREATE 后旧 Tunnel/Storage 写会话在事务内拒绝写入新表。
- Storage 隐式提交遇到未关闭 stream 返回冲突，避免成功终态丢弃暂存数据。
- 满 1024 个 block 时仍允许覆盖已有 block；新块受限。
- 过期实例下载会话回收；SIGTERM 等待进行中的 HTTP 请求排空。
- Arrow 解压分配额度与解码数据容量检查；重试历史设限。

资源边界：单次 payload/解码后估算暂存数据 64 MiB；Arrow 分配器 64 MiB；默认最多 64 个各类会话；batch/upsert 最多 1024 个块/pack；stream 重试历史最多 10000 条。实例结果估算总额 512 MiB，超额时回收较旧的已完成结果；下载快照另有 256 MiB 预算。这些是局部预算，不是进程 RSS 的硬限额，CI 容器应设置自己的内存限制。

## Flink 上传测试基座

已用 Flink 1.16.2 / connector 1.16.25 源码构建 / Java SDK 0.61.2-public / JDK17 的实际本地作业验证：standalone 有界输入的 Protobuf、Arrow 各覆盖普通表与动态分区表，结果逐行读取核对。配置 `sink.standalone.enable=true`；三层名称需 `odps.namespace.schema=true`（以所用 connector 的配置定义为准），服务和 Tunnel endpoint 都指向同一容器映射地址。

默认 coordinator 模式的有界任务曾出现任务 FINISHED、12 行未提交的失败；该 connector 路径的 endInput 仅设置结束标志，异步会话尚未就绪时任务可结束。RC 可以复现该问题，不能据任务成功认定上传成功。使用者应核对行值和提交结果。

可作为协议与上传正确性回归基座。尚未验证：checkpoint 故障恢复、跨作业 exactly-once、真实云端配额/鉴权/路由。Emulator 自身重启不保留 transfer session 和幂等历史，不能用来验收这些云端保证。CDC、Volume、Blob、Storage v1 仍不支持。

## 正式版本门禁

1. 同一候选源码在 Linux Docker 从源构建，并通过 Go race 与 Java SDK Testcontainers；发布 CI 的实际 run 另行记录。
2. 固定社区范围，确认上述功能限制可接受；Flink 默认 coordinator 的失败必须保留，不能对外宣称所有 sink 模式可用。
3. 审核最终公开 diff、许可证和文档；随后发布代码/tag/镜像，并从目标 registry 拉取后执行消费者测试。内部 Git 推送、本地 docker load 不代替公开发布。
