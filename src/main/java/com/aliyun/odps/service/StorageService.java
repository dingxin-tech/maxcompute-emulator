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

package com.aliyun.odps.service;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.entity.PlanSplitRequest;
import com.aliyun.odps.entity.PlanSplitResponse;
import com.aliyun.odps.entity.SqlLiteColumn;
import com.aliyun.odps.entity.TableData;
import com.aliyun.odps.entity.TableId;
import com.aliyun.odps.utils.CommonUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tech.dingxin.ArrowDataSerializer;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Service
public class StorageService {
    private Map<String, TableId> sessionIdTableMap;
    @Autowired
    private TableService tableService;

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    public StorageService() {
        this.sessionIdTableMap = new HashMap<>();
    }

    public PlanSplitResponse planSplit(PlanSplitRequest request) {
        try {
            String sessionId = CommonUtils.generateUUID();
            sessionIdTableMap.put(sessionId, TableId.of(request.getTable(), request.getRequiredPartitions()));
            List<SqlLiteColumn> schema = tableService.getDataSchema(request.getTable());
            if (request.getSplitMode().equals("RowOffset")) {
                long rowCount = tableService.getRowCount(request.getTable());
                return new PlanSplitResponse(sessionId, null, null, schema, rowCount, null);
            } else {
                return new PlanSplitResponse(sessionId, null, null, schema, null, 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new PlanSplitResponse(null, null, e.getMessage(), null, null, null);
        }
    }

    public void readTable(String table, String sessionId, Long maxBatchRows, Integer splitIndex,
                          OutputStream outputStream)
            throws Exception {
        TableData tableData = read(sessionIdTableMap.get(sessionId));
        Schema schema = tableData.getSchema();
        try (ArrowDataSerializer serializer = new ArrowDataSerializer(schema, allocator);
                ArrowStreamWriter arrowWriter = new ArrowStreamWriter(serializer.getVectorSchemaRoot(), null,
                        outputStream)) {
            arrowWriter.start();
            for (int i = 0; i < tableData.getData().size(); i++) {
                serializer.add(tableData.getData().get(i));
                if ((i + 1) % 4096 == 0) {
                    serializer.getVectorSchemaRoot();
                    arrowWriter.writeBatch();
                    serializer.reset();
                }
            }
            serializer.getVectorSchemaRoot();
            arrowWriter.writeBatch();
            // Finalize and close the writer.
            arrowWriter.end();
        }
    }

    private TableData read(TableId tableId) throws Exception {
        StringBuilder sql;
        if (tableId.getPartitionNames() == null && tableId.getPartitionName() == null) {
            sql = new StringBuilder("select * from " + tableId.getTableName().toUpperCase() + ";");
        } else if (tableId.getPartitionName() != null) {
            PartitionSpec partitionSpec = new PartitionSpec(tableId.getPartitionName());
            Set<String> keys = partitionSpec.keys();
            sql = new StringBuilder("select * from " + tableId.getTableName().toUpperCase() + " where ");
            for (String key : keys) {
                sql.append(key).append(" = '").append(partitionSpec.get(key)).append("' and ");
            }
            sql.delete(sql.length() - 4, sql.length());
            sql.append(";");
        } else {
            sql = new StringBuilder("select * from " + tableId.getTableName().toUpperCase() + " where ");
            List<String> partitionNames = tableId.getPartitionNames();
            for (String partitionName : partitionNames) {
                sql.append("(");
                PartitionSpec partitionSpec = new PartitionSpec(partitionName);
                Set<String> keys = partitionSpec.keys();
                for (String key : keys) {
                    sql.append(key).append(" = '").append(partitionSpec.get(key)).append("' and ");
                }
                sql.delete(sql.length() - 4, sql.length());

                sql.append(") or ");
            }
            sql.delete(sql.length() - 4, sql.length());
            sql.append(";");
        }
        try (
                Connection conn = CommonUtils.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql.toString());
        ) {
            return CommonUtils.convertToTableData(rs);
        }
    }
}
