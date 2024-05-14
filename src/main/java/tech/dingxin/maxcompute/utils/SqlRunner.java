package tech.dingxin.maxcompute.utils;

import com.csvreader.CsvWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SqlRunner {

    private static final String URL = "jdbc:sqlite:/tmp/maxcompute-emulator.db";
    private static final String DRIVER = "org.sqlite.JDBC";
    private static final String MODE = "always";

    static Connection connection;

    private static Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(URL);
        }
        return connection;
    }

    public static String execute(String sql) {

        try (Statement stmt = getConnection().createStatement()) {
            System.out.println("execute sql: " + sql);
            boolean hasResultSet = stmt.execute(sql);

            if (hasResultSet) {
                return processResultSet(stmt.getResultSet());
            } else {
                int updateCount = stmt.getUpdateCount();
                return "";
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return "";
        }
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
