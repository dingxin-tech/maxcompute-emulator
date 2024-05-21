/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps;

import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.type.TypeInfoFactory;
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
        TableSchema schema = odps.tables().get("project", "table1").getSchema();
        schema.getAllColumns().stream().forEach(c -> System.out.println(c.getName()));
    }

    @Test
    void testUpsertSession() throws Exception {
        Odps odps = getTestOdps();
        odps.tables().delete("students", true);
        odps.tables().newTableCreator("project", "students",
                TableSchema.builder().withColumn(Column.newBuilder("id", TypeInfoFactory.BIGINT).primaryKey().build())
                        .withStringColumn("name").build()).transactionTable().create();

        TableTunnel.UpsertSession session = odps.tableTunnel().buildUpsertSession("project", "students").build();

        UpsertStream stream = session.buildUpsertStream().setCompressOption(new CompressOption(
                CompressOption.CompressAlgorithm.ODPS_RAW, 1, 0)).build();
        Record record = session.newRecord();
        record.set(0, 2L);
        record.set(1, "Jack");
        stream.upsert(record);
        stream.upsert(record);
        stream.upsert(record);
        stream.delete(record);
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

    @Test
    void testExecuteAddColumnSql() throws OdpsException {
        Odps odps = getTestOdps();
        Instance run = SQLTask.run(odps,
                "alter table project.table1 add columns (id BIGINT comment 'BIGINT',name STRING comment 'STRING');");
        run.waitForSuccess();
    }

    @Test
    void testExecuteDropColumnSql() throws OdpsException {
        Odps odps = getTestOdps();
        Instance run = SQLTask.run(odps,
                "ALTER TABLE MOCKED_MC.TABLE1 DROP COLUMNS NEWCOL2;");
        run.waitForSuccess();
    }

    @Test
    void testExecuteChangeColumnSql() throws OdpsException {
        Odps odps = getTestOdps();
        Instance run = SQLTask.run(odps,
                "alter table project.table1 change column id2 id3 STRING;");
        run.waitForSuccess();
    }

    @Test
    void testExecuteRenameColumnSql() throws OdpsException {
        Odps odps = getTestOdps();
        Instance run = SQLTask.run(odps, "alter table mocked_mc.table1 change column col3 rename to newCol3;");
        run.waitForSuccess();
    }

    @Test
    void generateLogview() throws OdpsException {
        Odps odps = getTestOdps();
        Instance run = SQLTask.run(odps, "select 1;");
        System.out.println(odps.logview().generateLogView(run, 24));
    }

    @Test
    void testReloadTable() throws OdpsException {
        Odps odps = getTestOdps();
        Table table = odps.tables().get("project", "table1");
        boolean transactional = table.isTransactional();
        TableSchema schema = table.getSchema();
        boolean partitioned = table.isPartitioned();
        List<String> primaryKey = table.getPrimaryKey();

    }

}
