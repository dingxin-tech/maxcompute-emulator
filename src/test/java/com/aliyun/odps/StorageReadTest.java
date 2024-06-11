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
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.utils.CommonUtils;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class StorageReadTest {

    static Odps odps = MaxcomputeEmulatorApplicationTests.getTestOdps();
    static EnvironmentSettings environmentSettings =
            EnvironmentSettings.newBuilder().withDefaultProject(odps.getDefaultProject())
                    .withTunnelEndpoint(odps.getTunnelEndpoint()).withCredentials(
                            Credentials.newBuilder().withAccount(odps.getAccount()).build()).build();

    @BeforeAll
    static void beforeAll() throws OdpsException {
        CommonUtils.initEmulator();
    }

    @Test
    void testPlanSplits() throws IOException, OdpsException {
        odps.tables().delete("project", "TABLE1", true);
        odps.tables().create("project", "TABLE1",
                TableSchema.builder().withColumn(Column.newBuilder("id", TypeInfoFactory.BIGINT).build())
                        .withStringColumn("name").build());
        TableBatchReadSession tableBatchReadSession =
                new TableReadSessionBuilder().withSettings(environmentSettings)
                        .identifier(TableIdentifier.of("project", "TABLE1")).buildBatchReadSession();
        InputSplitAssigner inputSplitAssigner = tableBatchReadSession.getInputSplitAssigner();
        InputSplit[] allSplits = inputSplitAssigner.getAllSplits();
        for (InputSplit inputSplit : allSplits) {
            System.out.println(inputSplit);
        }
    }

    @Test
    void testReadTable() throws IOException, OdpsException {
        odps.tables().delete("project", "TABLE1", true);
        odps.tables().create("project", "TABLE1",
                TableSchema.builder().withColumn(Column.newBuilder("id", TypeInfoFactory.BIGINT).build())
                        .withStringColumn("name").build());
        TableBatchReadSession tableBatchReadSession =
                new TableReadSessionBuilder().withSettings(environmentSettings)
                        .identifier(TableIdentifier.of("project", "TABLE1")).buildBatchReadSession();
        InputSplitAssigner inputSplitAssigner = tableBatchReadSession.getInputSplitAssigner();
        InputSplit split = inputSplitAssigner.getAllSplits()[0];

        SplitReader<VectorSchemaRoot> arrowReader =
                tableBatchReadSession.createArrowReader(split,
                        ReaderOptions.newBuilder().withSettings(environmentSettings).build());
        while (arrowReader.hasNext()) {
            VectorSchemaRoot vectorSchemaRoot = arrowReader.get();
            System.out.println(vectorSchemaRoot.contentToTSVString());
        }
        arrowReader.close();
    }

    @Test
    void testReadTable2() throws IOException {
        TableBatchReadSession tableBatchReadSession =
                new TableReadSessionBuilder().withSettings(environmentSettings)
                        .identifier(TableIdentifier.of("project", "TABLE1")).withSplitOptions(
                                SplitOptions.newBuilder().SplitByRowOffset().build()).buildBatchReadSession();
        InputSplitAssigner inputSplitAssigner = tableBatchReadSession.getInputSplitAssigner();
        long totalRowCount = inputSplitAssigner.getTotalRowCount();
        InputSplit splitByRowOffset = inputSplitAssigner.getSplitByRowOffset(0, totalRowCount);

        SplitReader<VectorSchemaRoot> arrowReader =
                tableBatchReadSession.createArrowReader(splitByRowOffset,
                        ReaderOptions.newBuilder().withSettings(environmentSettings).build());
        while (arrowReader.hasNext()) {
            VectorSchemaRoot vectorSchemaRoot = arrowReader.get();
            System.out.println(vectorSchemaRoot.contentToTSVString());
        }
        arrowReader.close();
    }

    @Test
    void testVastTable() throws Exception {
        odps.tables().delete("project", "TABLE2", true);
        odps.tables().newTableCreator("project", "TABLE2",
                TableSchema.builder().withColumn(Column.newBuilder("id", TypeInfoFactory.BIGINT).primaryKey().build())
                        .withStringColumn("name").build()).ifNotExists().transactionTable().create();

        TableTunnel.UpsertSession session = odps.tableTunnel().buildUpsertSession("project", "TABLE2").build();
        UpsertStream stream = session.buildUpsertStream().setCompressOption(new CompressOption(
                CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0)).build();

        Record record = session.newRecord();
        for (long i = 0; i < 10000; i++) {
            record.set(0, i);
            record.set(1, "name" + i);
            stream.upsert(record);
        }
        stream.close();
        session.commit(false);

        TableBatchReadSession tableBatchReadSession =
                new TableReadSessionBuilder().withSettings(environmentSettings)
                        .identifier(TableIdentifier.of("project", "TABLE2")).buildBatchReadSession();
        InputSplitAssigner inputSplitAssigner = tableBatchReadSession.getInputSplitAssigner();
        InputSplit split = inputSplitAssigner.getAllSplits()[0];
        System.out.println(split);

        SplitReader<VectorSchemaRoot> arrowReader =
                tableBatchReadSession.createArrowReader(split,
                        ReaderOptions.newBuilder().withSettings(environmentSettings).build());
        while (arrowReader.hasNext()) {
            VectorSchemaRoot vectorSchemaRoot = arrowReader.get();
            System.out.println(vectorSchemaRoot.getRowCount());
        }
        arrowReader.close();
    }

    @Test
    void testReadPartitionedTable() throws OdpsException, IOException {
        odps.tables().delete("project", "partitionTable", true);
        TableSchema schema =
                TableSchema.builder().withColumn(Column.newBuilder("id", TypeInfoFactory.BIGINT).primaryKey().build())
                        .withStringColumn("name")
                        .withPartitionColumn(new Column("pt", TypeInfoFactory.STRING)).build();
        odps.tables().newTableCreator("project", "partitionTable", schema).transactionTable().ifNotExists().create();

        Table table = odps.tables().get("project", "partitionTable");
        TableSchema tableSchema = table.getSchema();
        System.out.println(tableSchema);

        TableTunnel.UpsertSession session =
                odps.tableTunnel().buildUpsertSession("project", "partitionTable").setPartitionSpec("pt=pt1").build();
        UpsertStream stream = session.buildUpsertStream().setCompressOption(new CompressOption(
                CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0)).build();

        Record record = session.newRecord();
        for (long i = 0; i < 10000; i++) {
            record.set(0, i);
            record.set(1, "name" + i);
            stream.upsert(record);
        }
        stream.close();
        session.commit(false);

        TableTunnel.UpsertSession session2 =
                odps.tableTunnel().buildUpsertSession("project", "partitionTable").setPartitionSpec("pt=pt2").build();
        UpsertStream stream2 = session2.buildUpsertStream().setCompressOption(new CompressOption(
                CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0)).build();

        Record record2 = session2.newRecord();
        for (long i = 0; i < 100; i++) {
            record2.set(0, i);
            record2.set(1, "name" + i);
            stream2.upsert(record2);
        }
        stream2.close();
        session2.commit(false);

        TableBatchReadSession tableBatchReadSession =
                new TableReadSessionBuilder().withSettings(environmentSettings)
                        .identifier(TableIdentifier.of("project", "partitionTable"))
                        .requiredPartitions(ImmutableList.of(new PartitionSpec("pt=pt2"))).buildBatchReadSession();
        InputSplitAssigner inputSplitAssigner = tableBatchReadSession.getInputSplitAssigner();
        InputSplit split = inputSplitAssigner.getAllSplits()[0];
        System.out.println(split);

        SplitReader<VectorSchemaRoot> arrowReader =
                tableBatchReadSession.createArrowReader(split,
                        ReaderOptions.newBuilder().withSettings(environmentSettings).build());
        while (arrowReader.hasNext()) {
            VectorSchemaRoot vectorSchemaRoot = arrowReader.get();
            Assertions.assertEquals(100, vectorSchemaRoot.getRowCount());
        }
        arrowReader.close();
    }
}
