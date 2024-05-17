package tech.dingxin.maxcompute;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Iterator;
import java.util.List;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class MaxcomputeEmulatorApplicationTests {

    private Odps getTestOdps() {
        Account account = new AliyunAccount("ak", "sk");
        Odps odps = new Odps(account);
        odps.setDefaultProject("project");
        odps.setEndpoint("http://127.0.0.1:8080");
        odps.setTunnelEndpoint("http://127.0.0.1:8080");
        return odps;
    }

    @Test
    void testInstance() throws OdpsException {
        Odps odps = getTestOdps();
        Instance instance2 = SQLTask.run(odps, "insert into students values(2, 'Jack');");
        instance2.waitForSuccess();

        Instance instance = SQLTask.run(odps, "select * from students;");
        instance.waitForSuccess();
        List<Record> result = SQLTask.getResult(instance);
        System.out.println(result);
    }

    @Test
    void getSchema() throws OdpsException {
        Odps odps = getTestOdps();
        TableSchema schema = odps.tables().get("project", "students").getSchema();
        schema.getAllColumns().stream().forEach(c -> System.out.println(c.getName()));
    }

    @Test
    void testUpsertSession() throws Exception {
        Odps odps = getTestOdps();
        TableTunnel.UpsertSession session = odps.tableTunnel().buildUpsertSession("project", "students").build();

        UpsertStream stream = session.buildUpsertStream().setCompressOption(new CompressOption(
                CompressOption.CompressAlgorithm.ODPS_RAW, 1, 0)).build();
        Record record = session.newRecord();
        record.set(0, 2);
        record.set(1, "Jack");
        stream.upsert(record);
        stream.upsert(record);
        stream.upsert(record);
        stream.upsert(record);
        stream.flush();

        session.commit(false);
    }

    @Test
    void testGetProject() throws Exception {
        Odps odps = getTestOdps();
        boolean flag =
                Boolean.parseBoolean(
                        odps.projects().get().getProperty("odps.schema.model.enabled"));
        System.out.println(flag);
    }

    @Test
    void testTableExist() throws OdpsException {
        Odps odps = getTestOdps();
        boolean exists = odps.tables().exists("students");
        System.out.println(exists);

        exists = odps.tables().exists("project");
        System.out.println(exists);
    }

    @Test
    void testShowTables() throws OdpsException {
        Odps odps = getTestOdps();
        Iterator<Table> iterator = odps.tables().iterator();
        while (iterator.hasNext()) {
            Table table = iterator.next();
            System.out.println(table.getName());
        }
    }

    @Test
    void testExecuteCreateTableSql() throws OdpsException {
        Odps odps = getTestOdps();

        Instance run = SQLTask.run(odps,
                "CREATE TABLE IF NOT EXISTS mocked_mc.table1 (`col1` STRING NOT NULL COMMENT 'STRING',`col2` STRING COMMENT 'STRING', PRIMARY KEY(`col1`)) " +
                        "TBLPROPERTIES('transactional'='true','write.bucket.num'='16');");
        run.waitForSuccess();
    }
}
