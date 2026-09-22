import static org.junit.jupiter.api.Assertions.*;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.task.SQLTask;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Java SDK acceptance for the MCQA / SQLRT session plane: an interactive executor must
 * really attach to a session on the emulator rather than quietly falling back to offline
 * instances, because the fallback would hide every session-protocol bug.
 *
 * <p>The observable for that is {@link SQLExecutor#isRunningInInteractiveMode()}: the SDK
 * flips it to false the moment attach fails, so asserting it is the difference between
 * "the session loop works" and "some other code path answered".
 */
public class McqaSessionTest {
  static GenericContainer<?> container;
  static String endpoint;
  Odps odps;
  String table;

  @BeforeAll
  static void start() {
    endpoint = System.getProperty("emulator.endpoint");
    if (endpoint == null) {
      container =
          new GenericContainer<>(
                  DockerImageName.parse(
                      System.getProperty("emulator.image", "maxcompute/maxcompute-emulator:ci")))
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
  void client() throws Exception {
    odps = new Odps(new AliyunAccount("test-ak", "test-sk"));
    odps.setDefaultProject("test_project");
    odps.setCurrentSchema("default");
    odps.setEndpoint(endpoint);
    table = "mcqa_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    SQLTask.run(odps, "create table " + table + "(id bigint, s string)").waitForSuccess();
    SQLTask.run(odps, "insert into " + table + " values (1,'one'),(2,'two')").waitForSuccess();
  }

  /**
   * An interactive executor that reads results through the session information channel.
   *
   * <p>{@code useInstanceTunnel(false)} is deliberate: it pins one read path down so a
   * regression on the other one cannot hide behind it. With the tunnel enabled the SDK
   * never asks the session for CSV text at all — see {@link #tunnelExecutor()}.
   */
  private SQLExecutor executor() throws OdpsException {
    return SQLExecutorBuilder.builder()
        .odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .useInstanceTunnel(false)
        .build();
  }

  /**
   * The fetch path the SDK uses by default, and the one JDBC's MaxQA mode takes: a
   * sub-query result is downloaded from the instance tunnel, and because a direct
   * download has no download session to ask, the schema has to travel in the stream.
   */
  private SQLExecutor tunnelExecutor() throws OdpsException {
    return SQLExecutorBuilder.builder()
        .odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .tunnelEndpoint(endpoint)
        .build();
  }

  @Test
  void defaultFetchDownloadsTheSubQueryResult() throws Exception {
    SQLExecutor executor = tunnelExecutor();
    try {
      executor.run("select id, s from " + table + " order by id", new HashMap<>());
      String sessionId = executor.getInstance().getId();
      java.util.List<Record> records = executor.getResult();
      assertEquals(2, records.size(), "rows over the instance tunnel");
      // Typed values, not the strings the information channel's CSV hands back: the
      // column types came from the in-stream schema, which only this read has.
      assertEquals(1L, ((Number) records.get(0).get(0)).longValue(), "bigint id");
      assertEquals("two", records.get(1).getString(1), "string s");
      assertEquals(2, records.get(0).getColumns().length, "schema from the stream");
      // The observable that separates "the tunnel answered" from "the SDK gave up":
      // a failed session download re-runs the statement offline, which replaces the
      // session instance with a new one and logs why.
      assertEquals(sessionId, executor.getInstance().getId(),
          "an offline fallback would replace the session instance");
      assertTrue(executor.isRunningInInteractiveMode(), "still interactive after the download");
      // QueryInfo writes "Will fallback to offline mode" when a read leaves the session,
      // and "Running in interactive mode" when it stays on one. getExecutionLog() drains,
      // so the list has to be read once.
      String logText = String.join("\n", executor.getExecutionLog());
      assertTrue(logText.contains("Running in interactive mode"),
          "the executor should report the session it ran on: " + logText);
      assertFalse(logText.contains("fallback"), "the download fell back: " + logText);
    } finally {
      executor.close();
    }
  }

  /**
   * The row count a direct download reports is what the SDK iterates, so it has to be the
   * rows still available from the requested offset — the total would send it reading past
   * the end of the result. An offset read is the only way to exercise that without a
   * ten-thousand-row statement in the acceptance suite.
   */
  @Test
  void offsetReadPagesTheSubQueryResult() throws Exception {
    SQLExecutor executor = tunnelExecutor();
    try {
      executor.run("select id from " + table + " order by id", new HashMap<>());
      String sessionId = executor.getInstance().getId();
      java.util.List<Record> tail = executor.getResult(1L, null, null);
      assertEquals(1, tail.size(), "rows from offset 1");
      assertEquals(2L, ((Number) tail.get(0).get(0)).longValue(), "the row after the offset");
      java.util.List<Record> window = executor.getResult(0L, 1L, null);
      assertEquals(1, window.size(), "a count limit is a count limit");
      assertEquals(1L, ((Number) window.get(0).get(0)).longValue());
      java.util.List<Record> rest = executor.getResult(2L, 10L, null);
      assertTrue(rest.isEmpty(), "reading past the end is empty, not an error");
      assertEquals(sessionId, executor.getInstance().getId(), "paging stayed on the session");
    } finally {
      executor.close();
    }
  }

  @Test
  void selectRunsThroughSessionNotOffline() throws Exception {
    SQLExecutor executor = executor();
    try {
      executor.run("select id, s from " + table + " order by id", new HashMap<>());
      List<Record> records = executor.getResult();
      assertTrue(executor.isRunningInInteractiveMode(),
          "the emulator must answer the SQLRT attach with a live session");
      assertEquals(2, records.size(), "sub query result rows");
      // The information channel returns CSV whose first line is the column-name header;
      // CSVRecordParser builds the schema from it, so wrong header handling shows up here.
      assertEquals("1", String.valueOf(records.get(0).get(0)));
      assertEquals("one", records.get(0).getString(1));
      assertEquals("two", records.get(1).getString(1));
      assertEquals(2, records.get(0).getColumns().length,
          "the sub query result must carry the column names from the CSV header");
      Instance session = executor.getInstance();
      assertNotNull(session, "an interactive executor is bound to a session instance");

      // A second statement reuses the same session instance and gets the next sub query id.
      executor.run("select id from " + table + " where id = 2", new HashMap<>());
      assertEquals(1, executor.getResult().size());
      assertEquals(session.getId(), executor.getInstance().getId(), "session instance must be reused");
    } finally {
      executor.close();
    }
  }

  @Test
  void failedStatementDoesNotKillTheSession() throws Exception {
    SQLExecutor executor = executor();
    try {
      executor.run("select id from " + table, new HashMap<>());
      assertEquals(2, executor.getResult().size(), "baseline statement should work");
      String sessionId = executor.getInstance().getId();

      // The submission is accepted; the failure surfaces with the sub query answer.
      executor.run("select * from " + table + "_missing", new HashMap<>());
      assertThrows(OdpsException.class, executor::getResult);

      // What the emulator must not do is drop the session: a bad statement is a bad
      // statement, not a dead worker. Verified server-side, because the SDK applies its
      // own fallback policy after an error and may keep the executor offline from here —
      // that is the SDK's decision, and the acceptance point here is the session state.
      assertEquals(Instance.Status.RUNNING, odps.instances().get(sessionId).getStatus(),
          "the session instance survives a failed sub query");
      executor.run("select s from " + table + " where id = 1", new HashMap<>());
      assertEquals("one", executor.getResult().get(0).getString(0));
    } finally {
      executor.close();
    }
  }

  @Test
  void closeStopsTheSessionInstance() throws Exception {
    SQLExecutor executor = executor();
    executor.run("select id from " + table, new HashMap<>());
    assertEquals(2, executor.getResult().size());
    String id = executor.getInstance().getId();
    assertEquals(Instance.Status.RUNNING, odps.instances().get(id).getStatus(),
        "the session instance stays Running while the executor holds it");
    executor.close();
    assertEquals(Instance.Status.TERMINATED, odps.instances().get(id).getStatus(),
        "close() must stop the session on the server side");
  }

  @Test
  void emptyResultStillCarriesSchema() throws Exception {
    SQLExecutor executor = executor();
    try {
      executor.run("select id, s from " + table + " where id = 99", new HashMap<>());
      // A header-only CSV parses to zero records but must not be reported as a failure.
      List<Record> records = executor.getResult();
      assertTrue(records.isEmpty(), "filtered-out rows must not fabricate a record");
      assertTrue(executor.isRunningInInteractiveMode());
    } finally {
      executor.close();
    }
  }
}
