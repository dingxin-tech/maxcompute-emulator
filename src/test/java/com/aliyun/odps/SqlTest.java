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
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class SqlTest {
    @Test
    void testReadTable() throws OdpsException {
        Odps odps = MaxcomputeEmulatorApplicationTests.getTestOdps();
        Instance instance = SQLTask.run(odps, "select * from table1;");
        instance.waitForSuccess();
        List<Record> result = SQLTask.getResult(instance);
        for (Record record : result) {
            System.out.println(record);
        }
    }

    @Test
    void testCreatePartitionedTable() throws OdpsException {
        Odps odps = MaxcomputeEmulatorApplicationTests.getTestOdps();
        odps.tables().newTableCreator(odps.getDefaultProject(), "parTest",
                        TableSchema.builder().withColumn(Column.newBuilder("id",
                                TypeInfoFactory.BIGINT).primaryKey().withComment("BIGINT").build()).withBigintColumn("id2").withPartitionColumn(
                                Column.newBuilder("ds", TypeInfoFactory.STRING).withComment("STRING").build()).build()).transactionTable().withHints(
                        ImmutableMap.of("hi1", "hi2")).withSchemaName("schema")
                .ifNotExists().withBucketNum(10).create();
        System.out.println(odps.tables().get(odps.getDefaultProject(), "parTest").getSchema());
    }

}
