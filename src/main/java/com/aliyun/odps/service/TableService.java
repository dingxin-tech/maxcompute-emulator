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

import com.aliyun.odps.entity.SqlLiteColumn;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.utils.CommonUtils;
import com.aliyun.odps.utils.SqlRunner;
import com.aliyun.odps.utils.TypeConvertUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Service
public class TableService {
    public String reloadTable(String tableName) throws SQLException {
        JsonObject table = new JsonObject();
        table.add("lastDDLTime", new JsonPrimitive(System.currentTimeMillis() / 1000));

        JsonArray columns = new JsonArray();
        JsonArray partitionKeys = new JsonArray();
        JsonArray primaryKey = new JsonArray();

        List<SqlLiteColumn> schema = getDataSchema(tableName);
        for (SqlLiteColumn column : schema) {
            columns.add(toJson(column.getName(), TypeConvertUtils.convertToMaxComputeType(column.getType()),
                    column.isNotNull(), column.getDefaultValue()));
            if (column.isPrimaryKey()) {
                primaryKey.add(new JsonPrimitive(column.getName()));
            }
        }

        List<SqlLiteColumn> prtitionSchema = getPartitionSchema(tableName);
        for (SqlLiteColumn column : prtitionSchema) {
            partitionKeys.add(toJson(column.getName(), TypeConvertUtils.convertToMaxComputeType(column.getType()),
                    column.isNotNull(), column.getDefaultValue()));
        }
        table.add("columns", columns);
        table.add("partitionKeys", partitionKeys);

        // reverse info
        JsonObject reverseInfo = new JsonObject();
        reverseInfo.add("Transactional", new JsonPrimitive("true"));
        reverseInfo.add("PrimaryKey", primaryKey);
        table.add("Reserved", new JsonPrimitive(reverseInfo.toString()));

        return table.toString();
    }

    public boolean tableExist(String tableName) {
        try {
            final String query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
            try (Connection conn = CommonUtils.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setString(1, tableName.toUpperCase());

                try (ResultSet resultSet = pstmt.executeQuery()) {
                    return resultSet.next();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public List<SqlLiteColumn> getDataSchema(String tableName) throws SQLException {
        if (StringUtils.isNotEmpty(tableName)) {
            return SqlRunner.getDataSchema(tableName);
        } else {
            return new ArrayList<>();
        }
    }

    public List<SqlLiteColumn> getPartitionSchema(String tableName) throws SQLException {
        if (StringUtils.isNotEmpty(tableName)) {
            return SqlRunner.getSchema(tableName).getPartitionColumns();
        } else {
            return new ArrayList<>();
        }
    }

    public List<String> listTables() {
        List<String> tables = new ArrayList<>();
        try (
                Connection conn = CommonUtils.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';")
        ) {
            while (rs.next()) {
                tables.add(rs.getString("name"));
            }
        } catch (SQLException e) {
            System.out.println("Database error:");
            e.printStackTrace();
        }
        return tables;
    }

    public long getRowCount(String tableName) throws SQLException {
        try (
                Connection conn = CommonUtils.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "select count(*) from " + tableName.toUpperCase() + ";")
        ) {
            return rs.getLong(1);
        }
    }

    private JsonObject toJson(String columnName, TypeInfo typeInfo, boolean notNull, String defaultValue) {
        JsonObject node = new JsonObject();
        node.addProperty("name", columnName);
        node.addProperty("type", typeInfo.getTypeName());
        node.addProperty("isNullable", !notNull);
        if (defaultValue != null) {
            node.addProperty("defaultValue", defaultValue);
        }
        return node;
    }
}
