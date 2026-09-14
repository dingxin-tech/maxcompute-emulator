import static org.junit.jupiter.api.Assertions.*;

import com.aliyun.odps.*;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrowRecordReader;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.*;
import java.util.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class EmulatorTest {
  static GenericContainer<?> container;
  static String endpoint;
  Odps odps;

  @BeforeAll
  static void start() {
    endpoint = System.getProperty("emulator.endpoint");
    if (endpoint == null) {
      container =
          new GenericContainer<>(
                  DockerImageName.parse(
                      System.getProperty("emulator.image", "maxcompute-emulator:1.0.0-rc.1")))
              .withExposedPorts(8080)
              .waitingFor(Wait.forHttp("/readyz"));
      container.start();
      endpoint = "http://" + container.getHost() + ":" + container.getMappedPort(8080);
    }
  }

  @AfterAll
  static void stop() {
    if (container != null) container.stop();
  }

  @BeforeEach
  void client() {
    odps = new Odps(new AliyunAccount("test-ak", "test-sk"));
    odps.setDefaultProject("test_project");
    odps.setCurrentSchema("default");
    odps.setEndpoint(endpoint);
    odps.setTunnelEndpoint(endpoint);
  }

  String table() {
    return "t_" + UUID.randomUUID().toString().replace("-", "");
  }

  void sql(String q) throws Exception {
    SQLTask.run(odps, q).waitForSuccess();
  }

  TableTunnel tunnel() {
    TableTunnel t = new TableTunnel(odps);
    t.setEndpoint(endpoint);
    return t;
  }

  String fixture() throws Exception {
    String t = table();
    sql("create table " + t + "(id bigint, name string, amount decimal(18,2), active boolean)");
    sql(
        "insert into "
            + t
            + " values(1,'AbC 中文',12.34,true),(2,'comma,quote',-56.78,false),(3,NULL,NULL,NULL)");
    return t;
  }

  @Test
  void sdkTableCreateDeleteAndIdentity() throws Exception {
    String t = table();
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("id", OdpsType.BIGINT));
    odps.tables().create("test_project", t, schema, "SDK table", false);
    assertTrue(odps.tables().exists(t));
    Table meta = odps.tables().get(t);
    meta.reload();
    assertEquals("SDK table", meta.getComment());
    String oldId = meta.getTableID();
    assertNotNull(oldId);
    assertFalse(oldId.isEmpty());
    odps.tables().delete(t);
    assertFalse(odps.tables().exists(t));
    odps.tables().create(t, schema);
    assertNotEquals(oldId, odps.tables().get(t).getTableID());
    odps.tables().delete(t);
  }

  @Test
  void partitionMetadataLifecycle() throws Exception {
    String t = table();
    sql("create table " + t + "(id bigint, name string) partitioned by(ds string, region string)");
    Table meta = odps.tables().get(t);
    var a = new PartitionSpec("ds='20260914',region='cn'");
    var b = new PartitionSpec("ds='20260915',region='us'");
    assertFalse(meta.hasPartition(a));
    meta.createPartition(a, true);
    meta.createPartition(a, true);
    assertTrue(meta.hasPartition(a));
    assertNotNull(meta.getPartition(a).getCreatedTime());
    assertEquals(1, meta.getPartitions().size());
    var u = tunnel().createUploadSession("test_project", t, b);
    try (var w = u.openRecordWriter(0)) {
      var row = u.newRecord();
      row.setBigint(0, 42L);
      row.setString(1, "flink");
      w.write(row);
    }
    u.commit(new Long[] {0L});
    assertTrue(meta.hasPartition(b));
    assertEquals(2, meta.getPartitions().size());
    meta.deletePartition(b);
    assertFalse(meta.hasPartition(b));
    assertEquals(1, meta.getPartitions().size());
    String pk = table();
    sql(
        "create table "
            + pk
            + "(id bigint not null, name string, primary key(id))"
            + " tblproperties('transactional'='true')");
    Table keyTable = odps.tables().get(pk);
    assertTrue(keyTable.isTransactional());
    assertEquals(List.of("id"), keyTable.getPrimaryKey());
    assertEquals("1", keyTable.getSchemaVersion());
  }

  @Test
  void metadataAndSQL() throws Exception {
    String t = fixture();
    assertEquals(4, odps.tables().get(t).getSchema().getColumns().size());
    assertEquals("test_project", odps.projects().get().getName());
    assertEquals(3, tunnel().createDownloadSession("test_project", t).getRecordCount());
  }

  @Test
  void protobufRawAndCompression() throws Exception {
    String t = fixture();
    for (var alg :
        new CompressOption.CompressAlgorithm[] {
          CompressOption.CompressAlgorithm.ODPS_RAW,
          CompressOption.CompressAlgorithm.ODPS_ZLIB,
          CompressOption.CompressAlgorithm.ODPS_ZSTD,
          CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME
        }) {
      var s = tunnel().createDownloadSession("test_project", t);
      try (TunnelRecordReader r = s.openRecordReader(0, 3, new CompressOption(alg, 1, 0))) {
        Record a = r.read();
        assertEquals(1L, a.getBigint(0));
        assertEquals("AbC 中文", a.getString(1));
        assertEquals(new BigDecimal("12.34"), a.getDecimal(2));
        assertTrue(a.getBoolean(3));
        assertEquals(2L, r.read().getBigint(0));
        Record n = r.read();
        assertNull(n.get(1));
        assertNull(n.get(2));
        assertNull(n.get(3));
        assertNull(r.read());
      }
    }
  }

  @Test
  void arrowRaw() throws Exception {
    String t = fixture();
    for (var alg :
        new CompressOption.CompressAlgorithm[] {
          CompressOption.CompressAlgorithm.ODPS_RAW,
          CompressOption.CompressAlgorithm.ODPS_ZLIB,
          CompressOption.CompressAlgorithm.ODPS_SNAPPY,
          CompressOption.CompressAlgorithm.ODPS_ARROW_LZ4_FRAME
        }) {
      var s = tunnel().createDownloadSession("test_project", t);
      try (ArrowRecordReader r = s.openArrowRecordReader(0, 3, new CompressOption(alg, 1, 0))) {
        VectorSchemaRoot root = r.read();
        assertNotNull(root);
        assertEquals(3, root.getRowCount());
        assertEquals(1L, root.getVector(0).getObject(0));
        assertEquals(new BigDecimal("12.34"), root.getVector(2).getObject(0));
        assertTrue(root.getVector(1).isNull(2));
        assertNull(r.read());
      }
    }
  }

  @Test
  void resumeProjectionAndRanges() throws Exception {
    String t = fixture();
    TableTunnel tt = tunnel();
    var s = tt.createDownloadSession("test_project", t);
    var resumed = tt.getDownloadSession("test_project", t, s.getId());
    assertEquals(3, resumed.getRecordCount());
    try (var reader =
        resumed.openRecordReader(
            1, 1, false, List.of(s.getSchema().getColumn(1), s.getSchema().getColumn(0)))) {
      Record r = reader.read();
      assertEquals("comma,quote", r.getString(0));
      assertEquals(2L, r.getBigint(1));
      assertNull(reader.read());
    }
  }

  @Test
  void partitionAndOverwriteSnapshot() throws Exception {
    String t = table();
    sql("create table " + t + "(id bigint, s string) partitioned by(ds string)");
    sql("insert into " + t + " partition(ds='a') values(1,'old')");
    sql("insert into " + t + " partition(ds='b') values(2,'other')");
    var old = tunnel().createDownloadSession("test_project", t, new PartitionSpec("ds='a'"));
    sql("insert overwrite table " + t + " partition(ds='a') values(3,'new')");
    try (var r = old.openRecordReader(0, 1)) {
      assertEquals(1L, r.read().getBigint(0));
    }
    try (var r =
        tunnel()
            .createDownloadSession("test_project", t, new PartitionSpec("ds='a'"))
            .openRecordReader(0, 1)) {
      assertEquals(3L, r.read().getBigint(0));
    }
    try (var r =
        tunnel()
            .createDownloadSession("test_project", t, new PartitionSpec("ds='b'"))
            .openRecordReader(0, 1)) {
      assertEquals(2L, r.read().getBigint(0));
    }
  }

  @Test
  void failedOverwriteRollsBack() throws Exception {
    String t = fixture();
    assertThrows(
        Exception.class, () -> sql("insert overwrite table " + t + " values('bad','x',1,true)"));
    assertEquals(3, tunnel().createDownloadSession("test_project", t).getRecordCount());
  }

  @Test
  void closeAndMissingSession() throws Exception {
    String t = fixture();
    var s = tunnel().createDownloadSession("test_project", t);
    var client = HttpClient.newHttpClient();
    String u = endpoint + "/projects/test_project/tables/" + t + "?downloadid=" + s.getId();
    assertEquals(
        200,
        client
            .send(
                HttpRequest.newBuilder(URI.create(u))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build(),
                HttpResponse.BodyHandlers.ofString())
            .statusCode());
    assertThrows(Exception.class, () -> tunnel().getDownloadSession("test_project", t, s.getId()));
    assertThrows(
        Exception.class, () -> tunnel().createDownloadSession("test_project", "missing_" + t));
  }

  @Test
  void emptyTable() throws Exception {
    String t = table();
    sql("create table " + t + "(id bigint)");
    var s = tunnel().createDownloadSession("test_project", t);
    assertEquals(0, s.getRecordCount());
    try (var r = s.openRecordReader(0, 0)) {
      assertNull(r.read());
    }
    try (var r = s.openArrowRecordReader(0, 0)) {
      assertNull(r.read());
    }
  }

  @Test
  void routerDiscovery() throws Exception {
    String t = fixture();
    Odps o = new Odps(new AliyunAccount("ak", "sk"));
    o.setEndpoint(endpoint);
    o.setDefaultProject("test_project");
    o.setCurrentSchema("default");
    TableTunnel tt = new TableTunnel(o);
    assertEquals(3, tt.createDownloadSession("test_project", t).getRecordCount());
  }

  @Test
  void complexTypesAndTime() throws Exception {
    String t = table();
    sql(
        "create table "
            + t
            + "(a array<bigint>,m map<string,bigint>,s struct<x:bigint,y:string>,dt datetime,ts"
            + " timestamp,d date)");
    sql(
        "insert into "
            + t
            + " select"
            + " array(1,2,NULL),map(array('z','a'),array(7,9)),named_struct('x',8,'y','Value'),datetime"
            + " '2026-09-14 12:34:56.123',timestamp '2026-09-14 12:34:56.123456789',date"
            + " '2026-09-14'");
    var s = tunnel().createDownloadSession("test_project", t);
    try (var r = s.openRecordReader(0, 1)) {
      Record v = r.read();
      assertEquals(Arrays.asList(1L, 2L, null), v.get(0));
      assertEquals(Map.of("z", 7L, "a", 9L), v.get(1));
      assertNotNull(v.get(2));
      assertEquals(
          java.time.Instant.parse("2026-09-14T12:34:56.123Z").toEpochMilli(),
          v.getDatetime(3).getTime());
      assertEquals(java.time.Instant.parse("2026-09-14T12:34:56.123456789Z"), v.get(4));
      try (var client = storage()) {
        var tid = com.aliyun.odps.table.TableIdentifier.of("test_project", t);
        var write = client.createTableWriteSessionBuilder(tid).build();
        try (var writer = write.createWriterBuilder("complex", 1).build().getAsRecordWriter(16)) {
          writer.write(v);
        }
        write.commit();
        var read = client.createTableReadSessionBuilder(tid).build();
        try (var reader =
            read.createReaderBuilder(read.getSplits().get(0)).build().getAsRecordReader()) {
          assertEquals(v.get(0), reader.read().get(0));
        }
      }

      assertNotNull(v.get(5));
      assertNull(r.read());
      var upload = tunnel().createUploadSession("test_project", t);
      try (var writer = upload.openRecordWriter(0)) {
        writer.write(v);
      }
      upload.commit(new Long[] {0L});
      var again = tunnel().createDownloadSession("test_project", t);
      assertEquals(3, again.getRecordCount());
      try (var reader = again.openRecordReader(1, 1)) {
        var copy = reader.read();
        assertEquals(v.get(0), copy.get(0));
        assertEquals(v.get(1), copy.get(1));
        assertEquals(v.get(4), copy.get(4));
      }
    }
    try (var r = s.openArrowRecordReader(0, 1)) {
      assertEquals(1, r.read().getRowCount());
      assertNull(r.read());
    }
  }

  @Test
  void arrowAcrossCRCChunksAndPagedReads() throws Exception {
    String t = table();
    sql("create table " + t + "(id bigint,s string)");
    List<String> values = new ArrayList<>();
    for (int i = 0; i < 2048; i++) values.add("(" + i + ",'" + "value".repeat(20) + "')");
    sql("insert into " + t + " values" + String.join(",", values));
    var session = tunnel().createDownloadSession("test_project", t);
    for (int start = 0; start < 2048; start += 1024) {
      try (var r = session.openArrowRecordReader(start, 1024)) {
        var root = r.read();
        assertEquals(1024, root.getRowCount());
        for (int i = 0; i < 1024; i++)
          assertEquals((long) (start + i), root.getVector(0).getObject(i));
        assertNull(r.read());
      }
    }
  }

  @Test
  void batchUploadCommitAndRetry() throws Exception {
    for (var alg :
        new CompressOption.CompressAlgorithm[] {
          CompressOption.CompressAlgorithm.ODPS_RAW,
          CompressOption.CompressAlgorithm.ODPS_ZLIB,
          CompressOption.CompressAlgorithm.ODPS_ZSTD,
          CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME,
          CompressOption.CompressAlgorithm.ODPS_SNAPPY
        }) {
      String name = table();
      sql("create table " + name + "(id bigint,s string)");
      var tt = tunnel();
      var upload = tt.createUploadSession("test_project", name);
      for (long block = 0; block < 2; block++) {
        try (var writer = upload.openRecordWriter(block, new CompressOption(alg, 1, 0))) {
          var row = upload.newRecord();
          row.setBigint(0, block);
          row.setString(1, "Hello 中文");
          writer.write(row);
        }
      }
      assertEquals(0, tt.createDownloadSession("test_project", name).getRecordCount());
      var resumed = tt.getUploadSession("test_project", name, upload.getId());
      assertEquals(2, resumed.getBlockList().length);
      resumed.commit(new Long[] {0L, 1L});
      resumed.commit(new Long[] {0L, 1L});
      var download = tt.createDownloadSession("test_project", name);
      assertEquals(2, download.getRecordCount());
      try (var reader = download.openRecordReader(0, 2)) {
        assertEquals("Hello 中文", reader.read().getString(1));
        assertEquals(1L, reader.read().getBigint(0));
      }
    }
  }

  @Test
  void arrowUploadAndCrossFormatDownload() throws Exception {
    for (var alg :
        new CompressOption.CompressAlgorithm[] {
          CompressOption.CompressAlgorithm.ODPS_RAW,
          CompressOption.CompressAlgorithm.ODPS_ZLIB,
          CompressOption.CompressAlgorithm.ODPS_SNAPPY,
          CompressOption.CompressAlgorithm.ODPS_ARROW_LZ4_FRAME
        }) {
      String name = table();
      sql("create table " + name + "(id bigint,s string)");
      var upload = tunnel().createUploadSession("test_project", name);
      try (var allocator = new org.apache.arrow.memory.RootAllocator();
          var root = VectorSchemaRoot.create(upload.getArrowSchema(), allocator)) {
        root.allocateNew();
        ((org.apache.arrow.vector.BigIntVector) root.getVector(0)).setSafe(0, 42);
        ((org.apache.arrow.vector.VarCharVector) root.getVector(1))
            .setSafe(0, "arrow 中文".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        root.setRowCount(1);
        try (var writer = upload.openArrowRecordWriter(0, new CompressOption(alg, 1, 0))) {
          writer.write(root);
        }
      }
      upload.commit(new Long[] {0L});
      var download = tunnel().createDownloadSession("test_project", name);
      try (var reader = download.openRecordReader(0, 1)) {
        assertEquals("arrow 中文", reader.read().getString(1));
        assertNull(reader.read());
      }
      try (var reader = download.openArrowRecordReader(0, 1)) {
        assertEquals(42L, reader.read().getVector(0).getObject(0));
      }
    }
  }

  @Test
  void streamUploadPack() throws Exception {
    String name = table();
    sql("create table " + name + "(id bigint,s string)");
    var session = tunnel().buildStreamUploadSession("test_project", name).build();
    var pack = session.newRecordPack();
    for (int i = 0; i < 3; i++) {
      var row = session.newRecord();
      row.setBigint(0, (long) i);
      row.setString(1, "stream");
      pack.append(row);
    }
    pack.flush();
    var download = tunnel().createDownloadSession("test_project", name);
    assertEquals(3, download.getRecordCount());
    try (var reader = download.openRecordReader(0, 3)) {
      for (int i = 0; i < 3; i++) assertEquals((long) i, reader.read().getBigint(0));
    }
  }

  @Test
  void upsertReplaceDeleteCommit() throws Exception {
    String name = table();
    sql(
        "create table "
            + name
            + "(id bigint not null,s string,primary key(id)) tblproperties"
            + " ('transactional'='true')");
    var session = tunnel().buildUpsertSession("test_project", name).build();
    try (var stream = session.buildUpsertStream().build()) {
      var row = session.newRecord();
      row.setBigint(0, 1L);
      row.setString(1, "before");
      stream.upsert(row);
      row.setString(1, "after");
      stream.upsert(row);
      row.setBigint(0, 2L);
      stream.upsert(row);
      stream.delete(row);
      stream.flush();
    }
    assertEquals(0, tunnel().createDownloadSession("test_project", name).getRecordCount());
    session.commit(false);
    session.close();
    var d = tunnel().createDownloadSession("test_project", name);
    assertEquals(1, d.getRecordCount());
    try (var r = d.openRecordReader(0, 1)) {
      var row = r.read();
      assertEquals(1L, row.getBigint(0));
      assertEquals("after", row.getString(1));
    }
  }

  com.aliyun.odps.storage.MaxStorageClient storage() {
    return com.aliyun.odps.storage.MaxStorageClient.builder()
        .endpoint(endpoint)
        .tunnelEndpoint(endpoint)
        .project("test_project")
        .credentialsProvider(
            new com.aliyun.odps.credentials.StaticCredentialProvider(
                odps.getAccount().getCredentials()))
        .build();
  }

  @Test
  void storageBatchAndStreaming() throws Exception {
    for (var mode :
        new com.aliyun.odps.storage.write.WriteMode[] {
          com.aliyun.odps.storage.write.WriteMode.BATCH,
          com.aliyun.odps.storage.write.WriteMode.STREAMING,
          com.aliyun.odps.storage.write.WriteMode.STREAMING_REALTIME
        }) {
      String t = table();
      sql("create table " + t + "(id bigint, s string)");
      var tableId = com.aliyun.odps.table.TableIdentifier.of("test_project", t);
      try (var client = storage()) {
        var session = client.createTableWriteSessionBuilder(tableId).withWriteMode(mode).build();
        try (var writer = session.createWriterBuilder("one", 1).build().getAsRecordWriter(16)) {
          for (long i = 0; i < 33; i++) {
            var record = writer.newRecord(true);
            record.setBigint(0, i);
            record.setString(1, "storage中文" + i);
            writer.write(record);
          }
        }
        if (mode == com.aliyun.odps.storage.write.WriteMode.BATCH) {
          assertEquals(0, tunnel().createDownloadSession("test_project", t).getRecordCount());
          session.commit();
          assertThrows(com.aliyun.odps.storage.ClientException.class, session::commit);
        }
        assertEquals(33, tunnel().createDownloadSession("test_project", t).getRecordCount());
        var read = client.createTableReadSessionBuilder(tableId).build();
        long count = 0;
        for (var split : read.getSplits()) {
          try (var reader = read.createReaderBuilder(split).build().getAsRecordReader()) {
            com.aliyun.odps.data.Record row;
            while ((row = reader.read()) != null) {
              assertEquals("storage中文" + row.getBigint(0), row.getString(1));
              count++;
            }
          }
        }
        assertEquals(33, count);
      }
    }
  }

  @Test
  void upsertPartialAndInstanceResults() throws Exception {
    String t = table();
    sql(
        "create table "
            + t
            + "(id bigint not null,s string,n bigint,primary key(id))"
            + " tblproperties('transactional'='true')");
    sql("insert into " + t + " values(1,'keep',10)");
    var up = tunnel().buildUpsertSession("test_project", t).build();
    try (var stream = up.buildUpsertStream().build()) {
      var row = up.newRecord();
      row.setBigint(0, 1L);
      row.setBigint(2, 99L);
      stream.upsert(row, java.util.List.of("n"));
      stream.flush();
    }
    up.commit(false);
    up.close();
    var instance = com.aliyun.odps.task.SQLTask.run(odps, "select id,s,n from " + t);
    instance.waitForSuccess();
    var it = new com.aliyun.odps.tunnel.InstanceTunnel(odps);
    it.setEndpoint(endpoint);
    var d = it.createDownloadSession("test_project", instance.getId());
    assertEquals(1, d.getRecordCount());
    try (var reader = d.openRecordReader(0, 1)) {
      var row = reader.read();
      assertEquals("keep", row.getString(1));
      assertEquals(99L, row.getBigint(2));
    }
    try (var client = storage()) {
      var read =
          client
              .createInstanceReadSessionBuilder(
                  com.aliyun.odps.table.InstanceIdentifier.of("test_project", instance.getId()))
              .build();
      assertEquals(1, read.getRecordCount());
      try (var reader = read.createReaderBuilder().build().getAsRecordReader()) {
        var row = reader.read();
        assertEquals("keep", row.getString(1));
        assertEquals(99L, row.getBigint(2));
        assertNull(reader.read());
      }
    }
  }

  @Test
  void storageExactlyOnceResumeAndAbort() throws Exception {
    String t = table();
    sql("create table " + t + "(id bigint,s string)");
    var tid = com.aliyun.odps.table.TableIdentifier.of("test_project", t);
    try (var client = storage()) {
      var session = client.createTableWriteSessionBuilder(tid).build();
      var writer =
          (com.aliyun.odps.storage.write.TableArrowWriter)
              session.createWriterBuilder("eo", 1).withExactlyOnceMode(true).build();
      try (var records = writer.getAsRecordWriter(16)) {
        for (long i = 0; i < 8; i++) {
          var row = records.newRecord(true);
          row.setBigint(0, i);
          row.setString(1, "eo");
          records.write(row);
        }
      }
      assertEquals(8, writer.getRowOffset());
      var resumed =
          client.createTableWriteSessionBuilder(tid).withSessionId(session.getId()).build();
      try (var w =
          (com.aliyun.odps.storage.write.TableArrowWriter)
              resumed
                  .createWriterBuilder("eo", 1)
                  .withExactlyOnceMode(true)
                  .withResume(true)
                  .build()) {
        assertEquals(8, w.getRowOffset());
      }
      session.commit();
      assertEquals(8, tunnel().createDownloadSession("test_project", t).getRecordCount());
      var aborted = client.createTableWriteSessionBuilder(tid).build();
      try (var records = aborted.createWriterBuilder("abort", 1).build().getAsRecordWriter(16)) {
        var row = records.newRecord(true);
        row.setBigint(0, 999L);
        row.setString(1, "abort");
        records.write(row);
      }
      aborted.abort();
      assertEquals(8, tunnel().createDownloadSession("test_project", t).getRecordCount());
    }
  }

  @Test
  void storagePartitionsProjectionAndOverwrite() throws Exception {
    String t = table();
    sql("create table " + t + "(id bigint,s string) partitioned by(ds string)");
    var tid = com.aliyun.odps.table.TableIdentifier.of("test_project", t);
    try (var client = storage()) {
      var write = client.createTableWriteSessionBuilder(tid).build();
      try (var writer = write.createWriterBuilder("dynamic", 1).build().getAsRecordWriter(16)) {
        for (long i = 0; i < 4; i++) {
          var row = writer.newRecord(true);
          row.setBigint(0, i);
          row.setString(1, "part" + i);
          row.setString(2, i < 2 ? "a" : "b");
          writer.write(row);
        }
      }
      write.commit();
      var overwrite =
          client
              .createTableWriteSessionBuilder(tid)
              .withPartition(new PartitionSpec("ds='a'"))
              .withOverwrite(true)
              .build();
      try (var writer = overwrite.createWriterBuilder("static", 1).build().getAsRecordWriter(16)) {
        var row = writer.newRecord(true);
        row.setBigint(0, 9L);
        row.setString(1, "new");
        writer.write(row);
      }
      overwrite.commit();
      assertEquals(
          1,
          tunnel()
              .createDownloadSession("test_project", t, new PartitionSpec("ds='a'"))
              .getRecordCount());
      assertEquals(
          2,
          tunnel()
              .createDownloadSession("test_project", t, new PartitionSpec("ds='b'"))
              .getRecordCount());
      var read =
          client
              .createTableReadSessionBuilder(tid)
              .withPartitions(java.util.List.of(new PartitionSpec("ds='b'")))
              .withColumns(java.util.List.of("s"))
              .withPartitionColumns(java.util.List.of("ds"))
              .withSplitOptions(
                  com.aliyun.odps.storage.settings.SplitOptions.newBuilder()
                      .withSplitRowCount(1)
                      .build())
              .build();
      int count = 0;
      for (var split : read.getSplits()) {
        try (var reader = read.createReaderBuilder(split).build().getAsRecordReader()) {
          com.aliyun.odps.data.Record row;
          while ((row = reader.read()) != null) {
            assertTrue(row.getString(0).startsWith("part"));
            assertEquals("b", row.getString(1));
            count++;
          }
        }
      }
      assertEquals(2, count);
    }
  }

  @Test
  void storageBatchCompatible() throws Exception {
    String t = table();
    sql("create table " + t + "(id bigint,s string)");
    var tid = com.aliyun.odps.table.TableIdentifier.of("test_project", t);
    try (var client = storage()) {
      var session =
          client
              .createTableWriteSessionBuilder(tid)
              .withWriteMode(com.aliyun.odps.storage.write.WriteMode.BATCH_COMPATIBLE)
              .build();
      var results = new java.util.ArrayList<com.aliyun.odps.storage.write.BlockWriteResult>();
      for (int i = 0; i < 2; i++) {
        try (var writer = session.createBlockWriter(i, 0);
            var root = writer.createVectorSchemaRoot()) {
          root.allocateNew();
          ((org.apache.arrow.vector.BigIntVector) root.getVector(0)).setSafe(0, i);
          ((org.apache.arrow.vector.VarCharVector) root.getVector(1))
              .setSafe(0, ("block" + i).getBytes());
          root.setRowCount(1);
          writer.writeBatch(root);
          results.add(writer.commit());
        }
      }
      assertEquals(0, tunnel().createDownloadSession("test_project", t).getRecordCount());
      session.commit(results);
      assertEquals(2, tunnel().createDownloadSession("test_project", t).getRecordCount());
    }
  }

  @Test
  void storagePrimaryKeyOperations() throws Exception {
    String t = table();
    sql(
        "create table "
            + t
            + "(id bigint not null,s string,primary key(id))"
            + " tblproperties('transactional'='true')");
    var tid = com.aliyun.odps.table.TableIdentifier.of("test_project", t);
    try (var client = storage()) {
      var session = client.createTableWriteSessionBuilder(tid).build();
      try (var writer =
              (com.aliyun.odps.storage.write.TableArrowWriter)
                  session.createWriterBuilder("pk", 1).build();
          var root = writer.createVectorSchemaRoot()) {
        root.allocateNew();
        long[] ids = {1, 1, 2, 2};
        String[] vals = {"before", "after", "delete", "delete"};
        for (int i = 0; i < 4; i++) {
          ((org.apache.arrow.vector.BigIntVector) root.getVector(0)).setSafe(i, ids[i]);
          ((org.apache.arrow.vector.VarCharVector) root.getVector(1))
              .setSafe(i, vals[i].getBytes());
          ((org.apache.arrow.vector.TinyIntVector) root.getVector("__operation"))
              .setSafe(i, i == 3 ? 'D' : 'U');
        }
        root.setRowCount(4);
        writer.writeBatch(root);
      }
      session.commit();
      var d = tunnel().createDownloadSession("test_project", t);
      assertEquals(1, d.getRecordCount());
      try (var reader = d.openRecordReader(0, 1)) {
        assertEquals("after", reader.read().getString(1));
      }
    }
  }

  @Test
  void emptyUploadsAndStoragePreview() throws Exception {
    String t = table();
    sql("create table " + t + "(id bigint,s string)");
    var upload = tunnel().createUploadSession("test_project", t);
    try (var writer = upload.openRecordWriter(0)) {}
    upload.commit(new Long[] {0L});
    var arrow = tunnel().createUploadSession("test_project", t);
    try (var writer = arrow.openArrowRecordWriter(0)) {}
    arrow.commit(new Long[] {0L});
    assertEquals(0, tunnel().createDownloadSession("test_project", t).getRecordCount());
    try (var client = storage()) {
      var tid = com.aliyun.odps.table.TableIdentifier.of("test_project", t);
      var read = client.createTableReadSessionBuilder(tid).build();
      for (var split : read.getSplits()) {
        try (var reader = read.createReaderBuilder(split).build()) {
          assertFalse(reader.nextBatch());
        }
      }
      try (var reader = client.previewTable(tid, null, null, 10)) {
        assertFalse(reader.nextBatch());
      }
      sql("insert into " + t + " values(1,'a'),(2,'b'),(3,'c')");
      try (var reader = client.previewTable(tid, null, java.util.List.of("s"), 2)) {
        assertTrue(reader.nextBatch());
        assertEquals(2, reader.getCurrentValue().getRowCount());
        assertEquals("s", reader.getSchema().getFields().get(0).getName());
        assertFalse(reader.nextBatch());
      }
    }
  }

  @Test
  void streamDynamicPartitions() throws Exception {
    String t = table();
    sql("create table " + t + "(id bigint,s string) partitioned by(ds string)");
    var session =
        tunnel().buildStreamUploadSession("test_project", t).setDynamicPartition(true).build();
    var pack = session.newRecordPack();
    for (int i = 0; i < 4; i++) {
      var row = (com.aliyun.odps.tunnel.impl.PartitionRecord) session.newRecord();
      row.setBigint(0, (long) i);
      row.setString(1, "dynamic");
      row.setPartition(new PartitionSpec(i < 2 ? "ds='a'" : "ds='b'"));
      pack.append(row);
    }
    pack.flush();
    for (String part : java.util.List.of("a", "b")) {
      assertEquals(
          2,
          tunnel()
              .createDownloadSession("test_project", t, new PartitionSpec("ds='" + part + "'"))
              .getRecordCount());
    }
  }
}
