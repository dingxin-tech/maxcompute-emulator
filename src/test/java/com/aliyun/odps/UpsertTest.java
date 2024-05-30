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

import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.type.TypeInfoFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static com.aliyun.odps.MaxcomputeEmulatorApplicationTests.getTestOdps;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class UpsertTest {
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

        Instance instance = SQLTask.run(odps, "select * from students;");
        instance.waitForSuccess();
        List<Record> result = SQLTask.getResult(instance);
        System.out.println(result);
    }

    @Test
    void testUpsertSessionWithPartition() throws Exception {
        Odps odps = getTestOdps();
        odps.tables().delete("students", true);
        odps.tables().newTableCreator("project", "students",
                        TableSchema.builder().withColumn(Column.newBuilder("id", TypeInfoFactory.BIGINT).primaryKey().build())
                                .withStringColumn("name")
                                .withPartitionColumn(Column.newBuilder("ds", TypeInfoFactory.STRING).build()).build())
                .transactionTable().withSchemaName("default").create();

        TableTunnel.UpsertSession session = odps.tableTunnel().buildUpsertSession("project", "students").setPartitionSpec("ds=1234").build();

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

        Instance instance = SQLTask.run(odps, "select * from students;");
        instance.waitForSuccess();
        List<Record> result = SQLTask.getResult(instance);
        System.out.println(result);
    }
}
