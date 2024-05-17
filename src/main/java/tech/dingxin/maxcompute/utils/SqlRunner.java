package tech.dingxin.maxcompute.utils;

import com.csvreader.CsvWriter;
import tech.dingxin.maxcompute.entity.SqlLiteColumn;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static tech.dingxin.maxcompute.utils.CommonUtils.getConnection;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SqlRunner {

    public static String execute(String originSql) throws SQLException {
        String sql = convertMCSqlToSQLite(originSql);

        try (Statement stmt = getConnection().createStatement()) {
            System.out.println("execute sql: " + sql);
            boolean hasResultSet = stmt.execute(sql);

            if (hasResultSet) {
                return processResultSet(stmt.getResultSet());
            } else {
                int updateCount = stmt.getUpdateCount();
                return "";
            }
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public static void upsertData(String tableName, List<Object[]> datas, List<SqlLiteColumn> schema) {
        // 构建基础的 INSERT 语句
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName);
        sql.append(" (");
        sql.append(String.join(", ", schema.stream().map(SqlLiteColumn::getName).toArray(String[]::new)));
        sql.append(") VALUES (");
        sql.append(String.join(", ", "?".repeat(schema.size()).split("")));
        sql.append(")");

        // 添加 ON CONFLICT ... DO UPDATE SET 子句
        sql.append(" ON CONFLICT (");
        sql.append(String.join(", ", schema.stream().filter(SqlLiteColumn::isPrimaryKey).map(SqlLiteColumn::getName)
                .toArray(String[]::new)));
        sql.append(") DO UPDATE SET ");

        // 排除 keyColumnNames，仅更新非键列
        for (SqlLiteColumn columns : schema) {
            if (!columns.isPrimaryKey()) {
                sql.append(columns.getName()).append(" = excluded.").append(columns.getName()).append(", ");
            }
        }
        // 移除最后的逗号和空格
        sql.setLength(sql.length() - 2);

        // 获取数据库连接并执行INSERT语句
        try (Connection conn = CommonUtils.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {

            for (Object[] rowData : datas) {
                // 绑定参数到PreparedStatement
                for (int i = 0; i < rowData.length; i++) {
                    pstmt.setObject(i + 1, rowData[i]);
                }
                // 添加到批次
                pstmt.addBatch();
            }
            // 执行INSERT操作
            pstmt.executeUpdate();
        } // try-with-resources将自动关闭资源
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String convertMCSqlToSQLite(String mcSql) {
        // Remove all COMMENT clauses considering backticks
        String noComments = mcSql.replaceAll("\\s+COMMENT\\s+'[^']+'", "");

        // Remove database prefix before the table name (if present)
        // \S+ matches one or more non-space characters
        // \. matches the literal dot character
        // This assumes that the database name does not contain spaces
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

        return withSemicolon;
    }

    public static String executeQuery(String sql) {
        try (Statement stmt = getConnection().createStatement()) {
            System.out.println("execute sql: " + sql);
            return processResultSet(stmt.executeQuery(sql));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
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
        }).collect(Collectors.toList());

        csvWriter.writeRecord(columns.toArray(new String[0]), true);
        csvWriter.setForceQualifier(false);

        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                Object value = resultSet.getObject(i);
                csvWriter.write(value.toString());
            }
            csvWriter.endRecord();
        }
        csvWriter.close();

        return writer.toString();
    }
}
