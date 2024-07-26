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
package com.aliyun.odps.utils;

import com.aliyun.odps.entity.SqlLiteColumn;
import com.aliyun.odps.entity.SqlLiteSchema;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class AnalyzeDbRunner {

    public static void main(String[] args) {
        String url = "jdbc:sqlite:/Users/dingxin/Downloads/TPC-H-small.db";
        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                Map<String, SqlLiteSchema> schemas = getDatabaseSchema(conn);
                schemas.forEach((tableName, schema) -> {
                    try {
                        SqlRunner.executeSql("INSERT INTO schemas VALUES ('" + tableName + "', '" + schema.toJson() +
                                "');");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, SqlLiteSchema> getDatabaseSchema(Connection conn) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rsTables = meta.getTables(null, null, "%", new String[] {"TABLE"});
        Map<String, SqlLiteSchema> schemas = new HashMap<>();

        while (rsTables.next()) {
            String tableName = rsTables.getString("TABLE_NAME");
            ResultSet rsColumns = meta.getColumns(null, null, tableName, "%");
            SqlLiteSchema schema = new SqlLiteSchema();
            List<SqlLiteColumn> columns = new ArrayList<>();

            while (rsColumns.next()) {
                String columnName = rsColumns.getString("COLUMN_NAME");
                String columnType = rsColumns.getString("TYPE_NAME");
                if (columnType.equals("TEXT")) {
                    columnType = "STRING";
                }
                boolean notNull = rsColumns.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls;
                String defaultValue = rsColumns.getString("COLUMN_DEF");
                boolean primaryKey = isPrimaryKey(meta, tableName, columnName);

                SqlLiteColumn column =
                        new SqlLiteColumn(columnName, columnType, notNull, defaultValue, primaryKey, false);
                columns.add(column);
            }

            schema.setColumns(columns);
            schema.setPartitionColumns(new ArrayList<>());
            schemas.put(tableName, schema);
            rsColumns.close();
        }

        rsTables.close();
        return schemas;
    }

    private static boolean isPrimaryKey(DatabaseMetaData meta, String tableName, String columnName)
            throws SQLException {
        ResultSet rsPrimaryKeys = meta.getPrimaryKeys(null, null, tableName);
        while (rsPrimaryKeys.next()) {
            String pkColumnName = rsPrimaryKeys.getString("COLUMN_NAME");
            if (columnName.equals(pkColumnName)) {
                rsPrimaryKeys.close();
                return true;
            }
        }
        rsPrimaryKeys.close();
        return false;
    }
}
