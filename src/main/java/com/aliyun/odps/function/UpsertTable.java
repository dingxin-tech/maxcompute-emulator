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

package com.aliyun.odps.function;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.entity.RowData;
import com.aliyun.odps.entity.SqlLiteColumn;
import com.aliyun.odps.entity.SqlLiteSchema;
import com.aliyun.odps.utils.CommonUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class UpsertTable {

    public static void upsertData(String tableName, List<RowData> datas, SqlLiteSchema schema, String partition)
            throws SQLException {
        if (StringUtils.isBlank(partition)) {
            upsertDataNoPartition(tableName, datas, schema.getColumns());
        } else {
            PartitionSpec partitionSpec = new PartitionSpec(partition.toUpperCase());
            List<SqlLiteColumn> partitionColumns = schema.getPartitionColumns();
            validate(partitionSpec, partitionColumns);

            List<RowData> newData = new ArrayList<>();
            List<SqlLiteColumn> allColumns = schema.getColumns();
            allColumns.addAll(partitionColumns);

            for (RowData data : datas) {
                Object[] objects = data.getData();
                Object[] newObjects = new Object[objects.length + partitionColumns.size()];
                System.arraycopy(objects, 0, newObjects, 0, objects.length);
                for (int index = 0; index < partitionColumns.size(); index++) {
                    newObjects[objects.length + index] = partitionSpec.get(partitionColumns.get(index).getName());
                }
                newData.add(new RowData(newObjects, data.getRowKind()));
            }
            upsertDataNoPartition(tableName, newData, allColumns);
        }
    }

    private static void validate(PartitionSpec partition, List<SqlLiteColumn> partitionColumns) {
        Set<String> keys = partition.keys();
        // check keys is equals to partitionColumns
        if (keys.size() != partitionColumns.size()) {
            throw new IllegalArgumentException(
                    "Partition spec " + partition + " is not valid for table " + partitionColumns);
        }
    }

    public static void upsertDataNoPartition(String tableName, List<RowData> datas, List<SqlLiteColumn> schema) {
        String[] uniqueKeys =
                schema.stream().filter(c -> c.isPrimaryKey() || c.isPartitionKey()).map(SqlLiteColumn::getName)
                        .toArray(String[]::new);
        // Building a basic INSERT statement
        StringBuilder upsertSql = new StringBuilder("INSERT INTO ");
        upsertSql.append(tableName.toUpperCase());
        upsertSql.append(" (");
        upsertSql.append(String.join(", ", schema.stream().map(SqlLiteColumn::getName).toArray(String[]::new)));
        upsertSql.append(") VALUES (");
        upsertSql.append(String.join(", ", "?".repeat(schema.size()).split("")));
        upsertSql.append(")");

        // Add ON CONFLICT ... DO UPDATE SET clause
        upsertSql.append(" ON CONFLICT (");
        upsertSql.append(String.join(", ", uniqueKeys));
        upsertSql.append(") DO UPDATE SET ");

        // Exclude keyColumnNames and update only non-key columns
        for (SqlLiteColumn columns : schema) {
            if (!columns.isPrimaryKey() && !columns.isPartitionKey()) {
                upsertSql.append(columns.getName()).append(" = EXCLUDED.").append(columns.getName()).append(", ");
            }
        }
        // Remove final comma and space
        upsertSql.setLength(upsertSql.length() - 2);

        StringBuilder deleteSql = new StringBuilder("DELETE FROM " + tableName + " WHERE ");
        for (int i = 0; i < uniqueKeys.length; i++) {
            deleteSql.append(uniqueKeys[i]).append(" = ?");
            if (i < uniqueKeys.length - 1) {
                deleteSql.append(" AND ");
            }
        }

        List<Object[]> toUpsert = new ArrayList<>();
        List<Object[]> toDelete = new ArrayList<>();
        for (RowData row : datas) {
            if (row.getRowKind() == RowData.RowKind.UPSERT) {
                executeBatch(toDelete, deleteSql);
                toUpsert.add(row.getData());
            } else if (row.getRowKind() == RowData.RowKind.DELETE) {
                executeBatch(toUpsert, upsertSql);
                toDelete.add(row.getPkData(schema));
            }
        }
        executeBatch(toDelete, deleteSql);
        executeBatch(toUpsert, upsertSql);
    }

    private static void executeBatch(List<Object[]> batch, StringBuilder upsertSql) {
        if (batch.isEmpty()) {
            return;
        }
        // Obtain the database connection and execute the INSERT statement
        try (Connection conn = CommonUtils.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(upsertSql.toString())) {

            for (Object[] rowData : batch) {
                for (int i = 0; i < rowData.length; i++) {
                    pstmt.setObject(i + 1, rowData[i]);
                }
                pstmt.addBatch();
            }
            System.out.println(pstmt);
            pstmt.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            batch.clear();
        }
    }
}
