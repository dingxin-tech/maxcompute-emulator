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

package tech.dingxin.maxcompute.utils;

import com.csvreader.CsvWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.dingxin.maxcompute.aspect.AccessAspect;
import tech.dingxin.maxcompute.entity.RowData;
import tech.dingxin.maxcompute.entity.SqlLiteColumn;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static tech.dingxin.maxcompute.utils.CommonUtils.getConnection;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SqlRunner {
    private static final Logger LOG = LoggerFactory.getLogger(AccessAspect.class);

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

    private static String executeSql(String sql) throws SQLException {
        try (Statement stmt = getConnection().createStatement()) {
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

    public static void upsertData(String tableName, List<RowData> datas, List<SqlLiteColumn> schema) {
        String[] primaryKeys = schema.stream().filter(SqlLiteColumn::isPrimaryKey).map(SqlLiteColumn::getName)
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
        upsertSql.append(String.join(", ", primaryKeys));
        upsertSql.append(") DO UPDATE SET ");

        // Exclude keyColumnNames and update only non-key columns
        for (SqlLiteColumn columns : schema) {
            if (!columns.isPrimaryKey()) {
                upsertSql.append(columns.getName()).append(" = EXCLUDED.").append(columns.getName()).append(", ");
            }
        }
        // Remove final comma and space
        upsertSql.setLength(upsertSql.length() - 2);

        StringBuilder deleteSql = new StringBuilder("DELETE FROM " + tableName + " WHERE ");
        for (int i = 0; i < primaryKeys.length; i++) {
            deleteSql.append(primaryKeys[i]).append(" = ?");
            if (i < primaryKeys.length - 1) {
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

    public static List<SqlLiteColumn> getSchema(String tableName) {
        List<SqlLiteColumn> columns = new ArrayList<>();
        try (Statement stmt = CommonUtils.getConnection().createStatement()) {
            ResultSet rs = stmt.executeQuery("PRAGMA table_info('" + tableName.toUpperCase() + "')");

            while (rs.next()) {
                String name = rs.getString("name");
                String type = rs.getString("type");
                boolean notnull = rs.getBoolean("notnull");
                String dfltValue = rs.getString("dflt_value");
                boolean pk = rs.getBoolean("pk");
                columns.add(new SqlLiteColumn(name, type, notnull, dfltValue, pk));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columns;
    }

    private static String handleDropTable(String originSql) throws SQLException {
        String noDatabasePrefix = originSql.replaceAll("DROP TABLE \\S+\\.", "DROP TABLE ");
        noDatabasePrefix = noDatabasePrefix.replaceAll("DROP TABLE IF EXISTS \\S+\\.", "DROP TABLE IF EXISTS ");
        return executeSql(noDatabasePrefix);
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
        for (int i = 0; i < columnDefinitions.length; i++) {
            String columnDefinition = columnDefinitions[i];
            // Adjust data types (if necessary)
            columnDefinition = columnDefinition.replace("STRING", "TEXT");
            alterTableStatements[i] = String.format("ALTER TABLE %s ADD COLUMN %s;", tableName, columnDefinition);
        }

        for (String alterTableStatement : alterTableStatements) {
            executeSql(alterTableStatement);
        }

        return "Success";
    }

    private static String handleCreateTable(String mcSql) throws SQLException {

        // Remove all COMMENT clauses considering backticks
        String noComments = mcSql.replaceAll("\\s+COMMENT\\s+'[^']+'", "");

        // Remove database prefix before the table name (if present)
        String noDatabasePrefix =
                noComments.replaceAll("CREATE TABLE IF NOT EXISTS \\S+\\.", "CREATE TABLE IF NOT EXISTS ");
        noDatabasePrefix = noDatabasePrefix.replaceAll("CREATE TABLE \\S+\\.", "CREATE TABLE ");

        // Remove unsupported TBLPROPERTIES part
        String noTblProperties = noDatabasePrefix.replaceAll("TBLPROPERTIES\\s*\\([^\\)]+\\)", "");

        // Adjust data types (if necessary)
        String sqliteCompatibleTypes = noTblProperties.replace("STRING", "TEXT");

        // Ensure the statement ends with a semicolon
        String withSemicolon = sqliteCompatibleTypes.trim();
        if (!withSemicolon.endsWith(";")) {
            withSemicolon += ";";
        }
        return executeSql(withSemicolon);
    }

    public static String handleDropColumn(String mcSql) throws SQLException {
        if (mcSql.endsWith(";")) {
            mcSql = mcSql.substring(0, mcSql.length() - 1);
        }
        String noDatabasePrefix = mcSql.replaceAll("ALTER TABLE \\S+\\.", "ALTER TABLE ");

        String tableName = noDatabasePrefix.replaceAll("ALTER TABLE (\\S+) DROP COLUMNS.*", "$1");
        String[] columnsToDrop =
                noDatabasePrefix.replaceAll("ALTER TABLE \\S+ DROP COLUMNS ", "").split(",");

        List<SqlLiteColumn> originSchema = getSchema(tableName);

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
        return "Success";
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
