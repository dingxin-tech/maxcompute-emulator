import static org.junit.jupiter.api.Assertions.*;

import com.aliyun.odps.*;
import com.aliyun.odps.account.*;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

public class ReliabilityTest {
  static GenericContainer<?> container;
  static String endpoint;

  @BeforeAll
  static void start() {
    String config =
        "{\"local\":{\"secret\":\"fixture-secret\",\"read\":[\"test_project.*\"],\"write\":[\"test_project.*\"]},\"sts\":{\"secret\":\"fixture-secret\",\"token\":\"fixture-token\",\"read\":[\"test_project.*\"],\"write\":[\"test_project.*\"]},\"denied\":{\"secret\":\"fixture-secret\",\"read\":[]},\"reader\":{\"secret\":\"fixture-secret\",\"read\":[\"test_project.*\"]}}";
    container =
        new GenericContainer<>(
                DockerImageName.parse(
                    System.getProperty("emulator.image", "maxcompute/maxcompute-emulator:1.1.0")))
            .withCopyToContainer(
                Transferable.of(config.getBytes(StandardCharsets.UTF_8), 0444), "/tmp/auth.json")
            .withCommand(
                "--listen",
                "0.0.0.0:8080",
                "--test-mode",
                "--test-network",
                "--quotas",
                "named",
                "--auth-mode",
                "strict",
                "--auth-config",
                "/tmp/auth.json")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/readyz"));
    container.start();
    endpoint = "http://" + container.getHost() + ":" + container.getMappedPort(8080);
  }

  @AfterAll
  static void stop() {
    if (container != null) {
      try {
        String logs = container.getLogs();
        assertFalse(logs.contains("fixture-secret"));
        assertFalse(logs.contains("fixture-token"));
        assertFalse(logs.contains("Authorization"));
      } finally {
        container.stop();
      }
    }
  }

  Odps client(Account a) {
    Odps o = new Odps(a);
    o.setDefaultProject("test_project");
    o.setEndpoint(endpoint);
    o.setTunnelEndpoint(endpoint);
    return o;
  }

  String fixture(Odps o) throws Exception {
    String t = "r_" + UUID.randomUUID().toString().replace("-", "");
    SQLTask.run(
            o,
            "create table "
                + t
                + "(id bigint,s string);insert into "
                + t
                + " values(1,'one'),(2,'two')")
        .waitForSuccess();
    return t;
  }

  TableTunnel tunnel(Odps o) {
    TableTunnel t = new TableTunnel(o);
    t.setEndpoint(endpoint);
    return t;
  }

  @Test
  void signedV2AndV4AndSTS() throws Exception {
    for (Account a :
        List.of(
            new AliyunAccount("local", "fixture-secret"),
            new AliyunAccount("local", "fixture-secret", "cn-test"),
            new StsAccount("sts", "fixture-secret", "fixture-token"),
            new StsAccount("sts", "fixture-secret", "fixture-token", "cn-test"))) {
      Odps o = client(a);
      String name = fixture(o);
      TableTunnel t = tunnel(o);
      var d = t.createDownloadSession("test_project", name);
      assertEquals(2, d.getRecordCount());
      try (TunnelRecordReader r = d.openRecordReader(0, 2)) {
        assertEquals(1L, r.read().getBigint(0));
        assertEquals(2L, r.read().getBigint(0));
        assertNull(r.read());
      }
    }
  }

  @Test
  void authenticationAndACLFailures() throws Exception {
    for (Account a :
        List.of(
            new AliyunAccount("missing", "fixture-secret"),
            new AliyunAccount("local", "wrong"),
            new StsAccount("sts", "fixture-secret", "wrong"))) {
      var e =
          assertThrows(
              OdpsException.class, () -> client(a).projects().get("test_project").reload());
      assertEquals("Unauthorized", e.getErrorCode());
    }
    var e =
        assertThrows(
            OdpsException.class,
            () ->
                client(new AliyunAccount("denied", "fixture-secret"))
                    .projects()
                    .get("test_project")
                    .reload());
    assertEquals("NoPermission", e.getErrorCode());
  }

  @Test
  void typeFixturesReadThroughSDK() throws Exception {
    Odps o = client(new AliyunAccount("local", "fixture-secret"));
    String t = "types_" + UUID.randomUUID().toString().replace("-", "");
    SQLTask.run(
            o,
            "create table "
                + t
                + "(ts timestamp_ntz,d date,dt datetime,n bigint,dec decimal(8,2),a array<bigint>,m"
                + " map<string,bigint>,s struct<x:struct<y:bigint>>);insert into "
                + t
                + " values(cast('1969-12-31 23:59:59.123456789' as timestamp_ntz),cast('1960-01-02'"
                + " as date),cast('1969-12-31 23:59:59.123' as"
                + " datetime),-9223372036854775807,12.345,array(),map(),named_struct('x',named_struct('y',7)))")
        .waitForSuccess();
    var d = tunnel(o).createDownloadSession("test_project", t);
    try (TunnelRecordReader r = d.openRecordReader(0, 1)) {
      Record row = r.read();
      assertEquals(Long.MIN_VALUE + 1, row.getBigint(3));
      assertEquals(new java.math.BigDecimal("12.35"), row.getDecimal(4));
      assertTrue(((List<?>) row.get(5)).isEmpty());
      assertTrue(((Map<?, ?>) row.get(6)).isEmpty());
      assertNotNull(row.get(0));
      assertEquals("1960-01-02", row.get(1).toString());
      assertNull(r.read());
    }
  }

  @Test
  void quotaAndInjected429() throws Exception {
    Odps o = client(new AliyunAccount("local", "fixture-secret"));
    String name = fixture(o);
    TableTunnel t = tunnel(o);
    t.getConfig().setQuotaName("named");
    var d = t.createDownloadSession("test_project", name);
    assertEquals("named", d.getQuotaName());
    t.getConfig().setQuotaName("absent");
    var e = assertThrows(OdpsException.class, () -> t.createDownloadSession("test_project", name));
    assertEquals("QuotaNotExist", e.getErrorCode());
    String rule =
        "{\"match\":{\"action\":\"create\",\"table\":\""
            + name
            + "\",\"quota\":\"named\"},\"effect\":{\"type\":\"http_error\",\"status\":429,\"code\":\"FlowExceeded\"},\"times\":1}";
    var response =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(endpoint + "/__test/faults/quota"))
                    .PUT(HttpRequest.BodyPublishers.ofString(rule))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode());
    t.getConfig().setQuotaName("named");
    try {
      d = t.createDownloadSession("test_project", name);
    } catch (OdpsException failure) {
      assertEquals("FlowExceeded", failure.getErrorCode());
      d = t.createDownloadSession("test_project", name);
    }
    assertEquals(2, d.getRecordCount());
  }

  // The metadata plane is the only place where a POST is a write with no Tunnel
  // session behind it, so the read-session "create" exception must not reach it.
  @Test
  void metadataPlaneWriteNeedsWriteGrant() throws Exception {
    Odps o = client(new AliyunAccount("reader", "fixture-secret"));
    String name = "readonly_" + UUID.randomUUID().toString().replace("-", "") + ".py";
    com.aliyun.odps.PyResource resource = new com.aliyun.odps.PyResource();
    resource.setName(name);
    var e =
        assertThrows(
            OdpsException.class,
            () ->
                o.resources()
                    .create(
                        resource,
                        new java.io.ByteArrayInputStream("print(1)".getBytes(StandardCharsets.UTF_8))));
    assertEquals("NoPermission", e.getErrorCode());
    assertFalse(o.resources().exists(name), "denied create must not publish a resource");
  }

  @Test
  void injectedMetadataCreateFailureLeavesNoPartialResource() throws Exception {
    Odps o = client(new AliyunAccount("local", "fixture-secret"));
    String name = "flaky_" + UUID.randomUUID().toString().replace("-", "") + ".py";
    String rule =
        "{\"match\":{\"plane\":\"rest\",\"object\":\"resources\",\"action\":\"create\"},\"effect\":{\"type\":\"http_error\",\"status\":500,\"code\":\"InternalError\"},\"times\":1}";
    var response =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(endpoint + "/__test/faults/metadata"))
                    .PUT(HttpRequest.BodyPublishers.ofString(rule))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode());
    com.aliyun.odps.PyResource resource = new com.aliyun.odps.PyResource();
    resource.setName(name);
    var e =
        assertThrows(
            OdpsException.class,
            () ->
                o.resources()
                    .create(
                        resource,
                        new java.io.ByteArrayInputStream("print(1)".getBytes(StandardCharsets.UTF_8))));
    assertEquals("InternalError", e.getErrorCode());
    assertFalse(o.resources().exists(name), "an injected failure must not publish a resource");
    // The hit budget is spent, so the client's own next attempt is a normal 201.
    o.resources()
        .create(resource, new java.io.ByteArrayInputStream("print(2)".getBytes(StandardCharsets.UTF_8)));
    assertTrue(o.resources().exists(name), "retry after the injected failure must succeed");
    try (java.io.InputStream in = o.resources().getResourceAsStream(name)) {
      var out = new java.io.ByteArrayOutputStream();
      for (int b; (b = in.read()) >= 0; ) out.write(b);
      assertEquals("print(2)", out.toString(StandardCharsets.UTF_8));
    }
    o.resources().delete(name);
  }

  @Test
  void rowDisconnectResumesWithoutDuplicates() throws Exception {
    Odps o = client(new AliyunAccount("local", "fixture-secret"));
    String name = fixture(o);
    var d = tunnel(o).createDownloadSession("test_project", name);
    String rule =
        "{\"match\":{\"action\":\"read\",\"table\":\""
            + name
            + "\",\"format\":\"protobuf\"},\"effect\":{\"type\":\"disconnect_after_rows\",\"rows\":1},\"times\":1}";
    var response =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(endpoint + "/__test/faults/rows"))
                    .PUT(HttpRequest.BodyPublishers.ofString(rule))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode());
    List<Long> ids = new ArrayList<>();
    try (TunnelRecordReader r = d.openRecordReader(0, 2)) {
      Record row;
      while ((row = r.read()) != null) ids.add(row.getBigint(0));
    }
    assertEquals(List.of(1L, 2L), ids);
  }

  @Test
  void strictStorageRead() throws Exception {
    Odps o = client(new StsAccount("sts", "fixture-secret", "fixture-token"));
    String name = fixture(o);
    try (var c =
        com.aliyun.odps.storage.MaxStorageClient.builder()
            .endpoint(endpoint)
            .tunnelEndpoint(endpoint)
            .project("test_project")
            .credentialsProvider(
                new com.aliyun.odps.credentials.StaticCredentialProvider(
                    o.getAccount().getCredentials()))
            .build()) {
      var table = com.aliyun.odps.table.TableIdentifier.of("test_project", name);
      var read = c.createTableReadSessionBuilder(table).build();
      List<Long> ids = new ArrayList<>();
      for (var split : read.getSplits()) {
        try (var reader = read.createReaderBuilder(split).build().getAsRecordReader()) {
          Record row;
          while ((row = reader.read()) != null) ids.add(row.getBigint(0));
        }
      }
      assertEquals(List.of(1L, 2L), ids);
    }
  }

  @Test
  void javaBigintMinimumRemainsAnExplicitClientBoundary() throws Exception {
    Odps o = client(new AliyunAccount("local", "fixture-secret"));
    String name = fixture(o);
    SQLTask.run(o, "insert overwrite table " + name + " values(-9223372036854775808,'minimum')")
        .waitForSuccess();
    var d = tunnel(o).createDownloadSession("test_project", name);
    try (TunnelRecordReader r = d.openRecordReader(0, 1)) {
      assertThrows(IllegalArgumentException.class, () -> r.read());
    }
  }
}
