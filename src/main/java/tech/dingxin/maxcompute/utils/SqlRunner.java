package tech.dingxin.maxcompute.utils;

import com.csvreader.CsvWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.dingxin.maxcompute.aspect.AccessAspect;
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

    public static void upsertData(String tableName, List<Object[]> datas, List<SqlLiteColumn> schema)
            throws SQLException {
        // Building a basic INSERT statement
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName.toUpperCase());
        sql.append(" (");
        sql.append(String.join(", ", schema.stream().map(SqlLiteColumn::getName).toArray(String[]::new)));
        sql.append(") VALUES (");
        sql.append(String.join(", ", "?".repeat(schema.size()).split("")));
        sql.append(")");

        // Add ON CONFLICT ... DO UPDATE SET clause
        sql.append(" ON CONFLICT (");
        sql.append(String.join(", ", schema.stream().filter(SqlLiteColumn::isPrimaryKey).map(SqlLiteColumn::getName)
                .toArray(String[]::new)));
        sql.append(") DO UPDATE SET ");

        // Exclude keyColumnNames and update only non-key columns
        for (SqlLiteColumn columns : schema) {
            if (!columns.isPrimaryKey()) {
                sql.append(columns.getName()).append(" = EXCLUDED.").append(columns.getName()).append(", ");
            }
        }
        // Remove final comma and space
        sql.setLength(sql.length() - 2);

        // Obtain the database connection and execute the INSERT statement
        try (Connection conn = CommonUtils.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {

            for (Object[] rowData : datas) {
                for (int i = 0; i < rowData.length; i++) {
                    pstmt.setObject(i + 1, rowData[i]);
                }
                LOG.info("execute sql: {}", pstmt);
                pstmt.addBatch();
            }
            pstmt.executeUpdate();
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
