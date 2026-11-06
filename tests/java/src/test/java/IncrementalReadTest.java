import static org.junit.jupiter.api.Assertions.*;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.TableIncrementalReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Consumer-side acceptance for incremental reads through the public table API — the
 * same surface the Flink and Spark connectors open: a version window, the version the
 * session says it covered, and the row-level event column a CDC reader branches on.
 */
public class IncrementalReadTest {
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
                      System.getProperty(
                          "emulator.image", "maxcompute/maxcompute-emulator:1.2.0")))
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

  EnvironmentSettings settings() {
    return EnvironmentSettings.newBuilder()
        .inAutoMode()
        .withCredentials(
            Credentials.newBuilder().withAccount(new AliyunAccount("test-ak", "test-sk")).build())
        .withDefaultProject("test_project")
        .withDefaultSchema("default")
        .withServiceEndpoint(endpoint)
        .withTunnelEndpoint(endpoint)
        .build();
  }

  void sql(String q) throws Exception {
    SQLTask.run(odps, q).waitForSuccess();
  }

  TableIncrementalReadSession open(
      String table, long startVersion, IncrementalOptions.IncrementalMode mode) throws Exception {
    return new TableReadSessionBuilder()
        .identifier(TableIdentifier.of("test_project", table))
        .requiredDataColumns(Arrays.asList("id", "s"))
        .withSettings(settings())
        .withIncrementalOptions(
            IncrementalOptions.newBuilder()
                .startVersion(startVersion)
                .withIncrementalMode(mode)
                .build())
        .buildIncrementalReadSession();
  }

  /** Reads every split of a session and returns the Arrow field order plus event codes. */
  List<Byte> ops(TableIncrementalReadSession session) throws Exception {
    List<Byte> out = new ArrayList<>();
    InputSplitAssigner assigner = session.getInputSplitAssigner();
    assertEquals(1, assigner.getSplitsCount(), "the emulator plans one logical split");
    for (int i = 0; i < assigner.getSplitsCount(); i++) {
      InputSplit split = assigner.getSplit(i);
      SplitReader<VectorSchemaRoot> reader =
          session.createArrowReader(split, ReaderOptions.newBuilder().withSettings(settings()).build());
      try {
        while (reader.hasNext()) {
          VectorSchemaRoot batch = reader.get();
          assertEquals(
              Arrays.asList("id", "s", "__meta_op_type", "__meta_timestamp"),
              batch.getSchema().getFields().stream()
                  .map(f -> f.getName())
                  .collect(Collectors.toList()),
              "events arrive as data columns and then the system columns");
          TinyIntVector column = (TinyIntVector) batch.getVector("__meta_op_type");
          assertNotNull(column, "__meta_op_type must arrive with the event rows");
          for (int r = 0; r < batch.getRowCount(); r++) {
            out.add(column.get(r));
          }
        }
      } finally {
        reader.close();
      }
    }
    return out;
  }

  String acidTable(String prefix) throws Exception {
    String t = prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    sql("create table " + t
        + "(id bigint, s string, primary key(id)) tblproperties('transactional'='true')");
    return t;
  }

  @Test
  void versionWindowAdvancesTheConsumerCursor() throws Exception {
    String t = acidTable("acid_window");
    sql("insert into " + t + " values (1,'a'),(2,'b')");

    TableIncrementalReadSession first = open(t, 0L, IncrementalOptions.IncrementalMode.CDC);
    Optional<Long> version = first.getSnapshot().getVersion();
    assertTrue(version.isPresent(), "an incremental session must report the version it covered");
    assertEquals(2L, version.get().longValue());
    assertEquals(Arrays.asList((byte) 2, (byte) 2), ops(first));

    sql("insert into " + t + " values (3,'c')");
    TableIncrementalReadSession second =
        open(t, version.get(), IncrementalOptions.IncrementalMode.CDC);
    assertEquals(
        version.get() + 1,
        second.getSnapshot().getVersion().get().longValue(),
        "the next window must cover the version added after the cursor");
    assertEquals(1, ops(second).size(), "only the row added after the cursor is published");
  }

  @Test
  void appendModePublishesInsertedContentOnly() throws Exception {
    String t = acidTable("acid_append");
    sql("insert into " + t + " values (1,'a'),(2,'b')");
    TableIncrementalReadSession appended =
        open(t, 0L, IncrementalOptions.IncrementalMode.APPEND);
    List<Byte> ops = ops(appended);
    assertEquals(2, ops.size());
    for (byte op : ops) {
      assertEquals(2, op, "append mode publishes inserted content only");
    }
  }

  @Test
  void nonAcidTableIsRefusedTheWayTheServiceRefusesIt() throws Exception {
    String t = "plain_" + UUID.randomUUID().toString().replace("-", "");
    sql("create table " + t + "(id bigint, s string)");
    Exception e =
        assertThrows(Exception.class, () -> open(t, 0L, IncrementalOptions.IncrementalMode.CDC));
    StringBuilder text = new StringBuilder();
    for (Throwable c = e; c != null; c = c.getCause()) {
      text.append(' ').append(c.getMessage());
    }
    assertTrue(
        text.toString().contains("ACID") || text.toString().contains("MethodNotAllowed"),
        "expected the service-shaped refusal, got: " + text);
  }
}
