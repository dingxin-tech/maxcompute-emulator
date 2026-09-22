import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Acceptance for the official JDBC driver, which is a harsher consumer than the Java SDK:
 * it mints a logview URL — and with it a bearer token — for every statement it submits, and
 * it opens a Tunnel download session for statements that produce no columns at all. Neither
 * of those shows up in an SDK test, and both used to end as a failed statement whose error
 * text named an unrelated endpoint.
 *
 * <p>Only offline mode is covered. The driver's MCQA path needs the session plane, which is
 * a separate milestone.
 */
public class JdbcDriverTest {
  static GenericContainer<?> container;
  static String endpoint;

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

  static String url(String... extra) {
    StringBuilder sb =
        new StringBuilder("jdbc:odps:")
            .append(endpoint)
            .append("?project=test_project&accessId=test-ak&accessKey=test-sk")
            .append("&charset=UTF-8&tunnelEndpoint=").append(endpoint);
    for (String kv : extra) sb.append('&').append(kv);
    return sb.toString();
  }

  static String freshTable(String prefix) {
    return prefix + "_" + (System.nanoTime() % 100000000L);
  }

  /**
   * The regression this whole file exists for: a statement with no result set. The driver
   * runs it, asks the frontend to sign a bearer token for the logview URL, and then opens a
   * Tunnel download session for the instance — which has no columns to describe.
   */
  @Test
  void statementsWithoutResultsAndStatementsWithThemBothRun() throws Exception {
    String table = freshTable("jdbc_round");
    try (Connection c = DriverManager.getConnection(url());
        Statement s = c.createStatement()) {
      s.execute("create table " + table + "(id bigint, s string)");
      s.execute("insert into " + table + " values (1,'one'),(2,'two'),(3,'three')");
      try (ResultSet rs = s.executeQuery("select id, s from " + table + " order by id")) {
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(2, meta.getColumnCount());
        assertEquals("BIGINT", meta.getColumnTypeName(1));
        String[] expected = {"one", "two", "three"};
        int rows = 0;
        while (rs.next()) {
          assertEquals(rows + 1, rs.getLong("id"), "order by id");
          assertEquals(expected[rows], rs.getString("s"));
          rows++;
        }
        assertEquals(3, rows, "one row per inserted tuple");
      }
      // A second statement on the same connection: every one of them mints its own token,
      // so a single per-statement failure would only show up from here on.
      try (ResultSet rs = s.executeQuery("select count(*) as cnt from " + table)) {
        assertTrue(rs.next());
        assertEquals(3, rs.getLong(1), "each statement must run exactly once");
      }
    }
  }

  /** A refused connection must explain itself: the driver repeats this text on later errors. */
  @Test
  void maxqaConnectionIsRefusedByName() {
    SQLException e =
        assertThrows(
            SQLException.class,
            () ->
                DriverManager.getConnection(
                    url("interactiveMode=maxqa", "quotaName=mcqa_quota", "disableFallback=true")));
    assertTrue(
        e.getMessage().contains("MCQA v2"),
        "the refusal should name the surface that is missing, got: " + e.getMessage());
    assertTrue(
        e.getMessage().contains("interactiveMode=mcqa"),
        "and point at the mode the emulator does simulate, got: " + e.getMessage());
  }

  /** LogView#getLogviewHost reads the body as one plain string. */
  @Test
  void logViewHostIsAnswered() throws Exception {
    HttpResponse<String> resp =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(endpoint + "/logview/host")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    assertEquals(200, resp.statusCode());
    assertTrue(resp.body().startsWith("http"), "body=" + resp.body());
  }
}
