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
import com.aliyun.odps.function.CreateTable;
import com.csvreader.CsvWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SqlRunner {
    private static final Logger LOG = LoggerFactory.getLogger(SqlRunner.class);

    public static String execute(String originSql) throws SQLException {
        if (originSql.toUpperCase().contains("CREATE TABLE")) {
            return handleCreateTable(originSql);
        } else if (originSql.toUpperCase().contains("ADD COLUMN")) {
            return handleAddColumn(originSql);
        } else if (originSql.toUpperCase().contains("DROP COLUMN")) {
            return handleDropColumn(originSql);
        } else if (originSql.toUpperCase().contains("CHANGE COLUMN")) {
            return handleRenameColumn(originSql);
        } else if (originSql.toUpperCase().contains("DROP TABLE")) {
            return handleDropTable(originSql);
        }
        return executeSql(originSql);
    }

    public static String executeSql(String sql) throws SQLException {
        try (Statement stmt = CommonUtils.getConnection().createStatement()) {
            LOG.info("execute sql: {}", sql);
            boolean hasResultSet = stmt.execute(sql);

            if (hasResultSet) {
                return processResultSet(stmt.getResultSet());
            } else {
                return "Success";
            }
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    private static String handleDropTable(String originSql) throws SQLException {
        if (originSql.startsWith("DROP TABLE IF EXISTS")) {
            originSql = originSql.replaceAll("DROP TABLE IF EXISTS \\S+\\.", "DROP TABLE IF EXISTS ");
            executeSql(originSql);
            String tableName = originSql.replaceAll("DROP TABLE IF EXISTS (\\S+);", "$1");
            executeSql("DELETE FROM schemas WHERE table_name = '" + tableName + "';");
        } else {
            originSql = originSql.replaceAll("DROP TABLE \\S+\\.", "DROP TABLE ");
            executeSql(originSql);
            String tableName = originSql.replaceAll("DROP TABLE (\\S+);", "$1");
            executeSql("DELETE FROM schemas WHERE table_name = '" + tableName + "';");
        }
        return "Success";
    }

    public static String handleAddColumn(String mcSql) throws SQLException {
        String noComments = mcSql.replaceAll("\\s+COMMENT\\s+'[^']+'", "");

        String noDatabasePrefix = noComments.replaceAll("ALTER TABLE \\S+\\.", "ALTER TABLE ");

        String columnsPart =
                noDatabasePrefix.replaceAll("ALTER TABLE \\S+ ADD COLUMNS \\(", "").replaceAll("\\);$", "");

        // split on whitespace to get column name
        String[] columnDefinitions = columnsPart.split("\\s*,\\s*");
        String tableName = noDatabasePrefix.replaceAll("ALTER TABLE (\\S+) ADD COLUMNS.*", "$1");

        String[] alterTableStatements = new String[columnDefinitions.length];

        SqlLiteSchema schema = getSchema(tableName);
        List<SqlLiteColumn> columns = schema.getColumns();

        for (int i = 0; i < columnDefinitions.length; i++) {
            String columnDefinition = columnDefinitions[i];
            // Adjust data types (if necessary)
            columnDefinition = columnDefinition.replace("STRING", "TEXT");
            alterTableStatements[i] = String.format("ALTER TABLE %s ADD COLUMN %s;", tableName, columnDefinition);

            String[] split = columnDefinition.trim().split("\\s+");
            columns.add(new SqlLiteColumn(split[0], split[1], false, null, false, false));
        }

        for (String alterTableStatement : alterTableStatements) {
            executeSql(alterTableStatement);
        }
        updateSchema(tableName, new SqlLiteSchema(columns, schema.getPartitionColumns()));
        return "Success";
    }

    private static String handleCreateTable(String mcSql) throws SQLException {
        CreateTable.execute(mcSql);
        return "Success";
    }

    public static String handleDropColumn(String mcSql) throws SQLException {
        if (mcSql.endsWith(";")) {
            mcSql = mcSql.substring(0, mcSql.length() - 1);
        }
        String noDatabasePrefix = mcSql.replaceAll("ALTER TABLE \\S+\\.", "ALTER TABLE ");

        String tableName = noDatabasePrefix.replaceAll("ALTER TABLE (\\S+) DROP COLUMNS.*", "$1");
        String[] columnsToDrop =
                noDatabasePrefix.replaceAll("ALTER TABLE \\S+ DROP COLUMNS ", "").split(",");

        List<SqlLiteColumn> originSchema = getDataSchema(tableName);

        Set<String> dropSet = Arrays.stream(columnsToDrop).collect(Collectors.toSet());

        StringBuilder createTableSql = new StringBuilder("CREATE TABLE temp_" + tableName + " (");
        StringBuilder insertTableSql = new StringBuilder("INSERT INTO temp_" + tableName + " SELECT ");
        for (SqlLiteColumn column : originSchema) {
            if (dropSet.contains(column.getName().toUpperCase())) {
                continue;
            }
            createTableSql.append(column.getName()).append(" ").append(column.getType());
            insertTableSql.append(column.getName()).append(",");
            if (column.isPrimaryKey()) {
                createTableSql.append(" PRIMARY KEY");
            }
            createTableSql.append(",");
        }
        createTableSql.deleteCharAt(createTableSql.length() - 1);
        insertTableSql.deleteCharAt(insertTableSql.length() - 1);
        createTableSql.append(");");
        insertTableSql.append(" FROM " + tableName + ";");

        executeSql(createTableSql.toString());
        executeSql(insertTableSql.toString());

        String dropOldTableSQL = "DROP TABLE " + tableName + ";";
        String renameTableSQL = "ALTER TABLE temp_" + tableName + " RENAME TO " + tableName + ";";

        executeSql(dropOldTableSQL);
        executeSql(renameTableSQL);

        SqlLiteSchema schema = getSchema(tableName);
        List<SqlLiteColumn> columns = schema.getColumns();
        columns.removeIf(c -> dropSet.contains(c.getName().toUpperCase()));
        updateSchema(tableName, new SqlLiteSchema(columns, schema.getPartitionColumns()));

        return "Success";
    }

    public static String handleRenameColumn(String mcSql) throws SQLException {
        String noDatabasePrefix = mcSql.replaceAll("ALTER TABLE \\S+\\.", "ALTER TABLE ");

        String tableName = noDatabasePrefix.replaceAll("ALTER TABLE (\\S+) CHANGE COLUMN.*", "$1");
        String[] columnToRename =
                noDatabasePrefix.replaceAll("ALTER TABLE \\S+ CHANGE COLUMN ", "").replace("\\s+", "").split(" ");
        String oldName;
        String newName;
        if (columnToRename[1].equals("RENAME")) {
            oldName = columnToRename[0];
            newName = columnToRename[3].replace(";", "");
        } else {
            oldName = columnToRename[0];
            newName = columnToRename[1];
        }

        if (oldName.equals(newName)) {
            return "Column name not changed";
        }
        String renameColumnSql = String.format("ALTER TABLE %s RENAME COLUMN %s TO %s;", tableName, oldName,
                newName);
        executeSql(renameColumnSql);

        SqlLiteSchema schema = getSchema(tableName);
        List<SqlLiteColumn> columns = schema.getColumns();
        for (SqlLiteColumn column : columns) {
            if (column.getName().equals(oldName)) {
                column.setName(newName);
            }
        }
        updateSchema(tableName, new SqlLiteSchema(columns, schema.getPartitionColumns()));
        return "Success";
    }

    public static SqlLiteSchema getSchema(String tableName) throws SQLException {
        try (Statement stmt = CommonUtils.getConnection().createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT schema FROM schemas WHERE table_name = '" + tableName.toUpperCase() + "';");
            if (rs.next()) {
                String schema = rs.getString("schema");
                return SqlLiteSchema.fromJson(schema);
            }
        }
        throw new SQLException("Table schema " + tableName + " not found");
    }

    static void updateSchema(String tableName, SqlLiteSchema schema) throws SQLException {
        try (Statement stmt = CommonUtils.getConnection().createStatement()) {
            stmt.executeUpdate(
                    "UPDATE schemas SET schema = '" + schema.toJson() + "' WHERE table_name = '" + tableName +
                            "';");
        }
    }

    public static List<SqlLiteColumn> getDataSchema(String tableName) throws SQLException {
        return getSchema(tableName).getColumns();
    }

    private static String processResultSet(ResultSet resultSet) throws SQLException, IOException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        StringWriter writer = new StringWriter();
        CsvWriter csvWriter = new CsvWriter(writer, ',');

        csvWriter.setForceQualifier(true);

        List<String> columns = IntStream.range(1, columnCount + 1).mapToObj(i -> {
            try {
                return metaData.getColumnName(i);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }).toList();

        csvWriter.writeRecord(columns.toArray(new String[0]), true);
        csvWriter.setForceQualifier(false);

        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                Object value = resultSet.getObject(i);
                if (value == null) {
                    csvWriter.write("NULL");
                } else {
                    csvWriter.write(value.toString());
                }
            }
            csvWriter.endRecord();
        }
        csvWriter.close();

        return writer.toString();
    }
}
