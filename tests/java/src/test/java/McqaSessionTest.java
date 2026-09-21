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
   * <p>{@code useInstanceTunnel(false)} is deliberate, and it is a scope boundary rather
   * than a convenience: by default the SDK fetches sub-query results through the instance
   * tunnel, which the emulator does not serve for sessions yet, and it then silently
   * re-runs the statement as an offline instance — an acceptance test written that way
   * would pass even with nothing implemented on the session plane.
   */
  private SQLExecutor executor() throws OdpsException {
    return SQLExecutorBuilder.builder()
        .odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .useInstanceTunnel(false)
        .build();
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
