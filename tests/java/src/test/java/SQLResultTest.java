import static org.junit.jupiter.api.Assertions.*;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.utils.CSVRecordParser;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** Covers the offline CSV API used by SeaTunnel's MaxCompute E2E assertions. */
public class SQLResultTest {
  static GenericContainer<?> container;
  static String endpoint;
  Odps odps;

  @BeforeAll
  static void start() {
    endpoint = System.getProperty("emulator.endpoint");
    if (endpoint == null) {
      container = new GenericContainer<>(
          System.getProperty("emulator.image", "maxcompute/maxcompute-emulator:1.1.0"))
          .withExposedPorts(8080).waitingFor(Wait.forHttp("/readyz"));
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
    odps = new Odps(new AliyunAccount("ak", "sk"));
    odps.setDefaultProject("test_project");
    odps.setEndpoint(endpoint);
    odps.setTunnelEndpoint(endpoint);
  }

  Instance sql(String statement) throws Exception {
    Instance instance = SQLTask.run(odps, statement);
    instance.waitForSuccess();
    return instance;
  }

  @Test
  void bufferedBatchUploadRetainsAllThreeRowsInSQLResult() throws Exception {
    String table = "csv_" + UUID.randomUUID().toString().replace("-", "");
    try {
      sql("create table " + table + " (id INT, name STRING, age INT)");
      TableTunnel tunnel = new TableTunnel(odps);
      tunnel.setEndpoint(endpoint);
      TableTunnel.UploadSession upload = tunnel.createUploadSession("test_project", table);
      try (var writer = upload.openBufferedWriter()) {
        for (int i = 1; i <= 3; i++) {
          Record row = upload.newRecord();
          row.set(0, i);
          row.setString(1, "INSERT_TEST" + i);
          row.set(2, 10 + i * 10);
          writer.write(row);
        }
      }
      upload.commit();
      assertEquals(3, tunnel.createDownloadSession("test_project", table).getRecordCount());
      List<Record> rows = SQLTask.getResult(sql("select * from " + table + " order by id"));
      assertEquals(3, rows.size());
      for (int i = 0; i < 3; i++) {
        assertEquals(String.valueOf(i + 1), rows.get(i).getString(0));
        assertEquals("INSERT_TEST" + (i + 1), rows.get(i).getString(1));
        assertEquals(String.valueOf(20 + i * 10), rows.get(i).getString(2));
      }
      assertEquals("id", rows.get(0).getColumns()[0].getName());
    } finally {
      odps.tables().delete("test_project", table, true);
    }
  }

  @Test
  void emptySelectRetainsItsColumnSchema() throws Exception {
    Instance instance = sql("select 1 as id, 'empty' as name where 1 = 0");
    assertTrue(SQLTask.getResult(instance).isEmpty());
    var parsed = CSVRecordParser.parse(instance.getTaskResults().values().iterator().next());
    assertEquals("id", parsed.getSchema().getColumn(0).getName());
    assertEquals("name", parsed.getSchema().getColumn(1).getName());
    assertTrue(parsed.getRecords().isEmpty());
  }

  @Test
  void quotedColumnNamesAndValuesRoundTripThroughXMLAndCSV() throws Exception {
    Instance instance = sql("select 'value,one' as `a,b`, 'value\"two' as `a\"b`, "
        + "'line\nvalue' as `line\nname`, cast(null as string) as missing, '' as blank, '<&>' as xml");
    List<Record> rows = SQLTask.getResult(instance);
    assertEquals(1, rows.size());
    Record row = rows.get(0);
    assertEquals("a,b", row.getColumns()[0].getName());
    assertEquals("a\"b", row.getColumns()[1].getName());
    assertEquals("line\nname", row.getColumns()[2].getName());
    assertEquals("value,one", row.getString(0));
    assertEquals("value\"two", row.getString(1));
    assertEquals("line\nvalue", row.getString(2));
    assertEquals("\\N", row.getString(3));
    assertEquals("", row.getString(4));
    assertEquals("<&>", row.getString(5));
  }
}
