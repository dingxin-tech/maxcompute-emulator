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
                      System.getProperty("emulator.image", "maxcompute-emulator:2.0.0-ck.1")))
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
        new CompressOption.CompressAlgorithm[] {CompressOption.CompressAlgorithm.ODPS_RAW}) {
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
            + " array(1,2,NULL),map(array('a'),array(7)),named_struct('x',8,'y','Value'),datetime"
            + " '2026-09-14 12:34:56.123',timestamp '2026-09-14 12:34:56.123456789',date"
            + " '2026-09-14'");
    var s = tunnel().createDownloadSession("test_project", t);
    try (var r = s.openRecordReader(0, 1)) {
      Record v = r.read();
      assertEquals(Arrays.asList(1L, 2L, null), v.get(0));
      assertEquals(Map.of("a", 7L), v.get(1));
      assertNotNull(v.get(2));
      assertEquals(
          java.time.Instant.parse("2026-09-14T12:34:56.123Z").toEpochMilli(),
          v.getDatetime(3).getTime());
      assertEquals(java.time.Instant.parse("2026-09-14T12:34:56.123456789Z"), v.get(4));
      assertNotNull(v.get(5));
      assertNull(r.read());
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
}
