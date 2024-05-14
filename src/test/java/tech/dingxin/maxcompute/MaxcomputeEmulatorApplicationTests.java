package tech.dingxin.maxcompute;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class MaxcomputeEmulatorApplicationTests {

    private Odps getTestOdps() {
        Account account = new AliyunAccount("ak", "sk");
        Odps odps = new Odps(account);
        odps.setDefaultProject("project");
        odps.setEndpoint("http://127.0.0.1:8080");
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
}
