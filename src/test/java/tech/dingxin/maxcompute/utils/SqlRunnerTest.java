package tech.dingxin.maxcompute.utils;

import org.junit.jupiter.api.Test;
import tech.dingxin.maxcompute.service.TableService;

import java.sql.SQLException;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SqlRunnerTest {
    @Test
    public void testCreateTable() throws SQLException {
        String sqlCreateTable = "CREATE TABLE IF NOT EXISTS students (\n"
                + " id integer PRIMARY KEY,\n"
                + " name text NOT NULL\n"
                + ");";
        SqlRunner.execute(sqlCreateTable);

        String sqlInsert = "INSERT INTO students (id, name) VALUES (1, 'John');";
        SqlRunner.execute(sqlInsert);

        String sqlSelect = "SELECT * FROM students;";
        String resultSet = SqlRunner.executeQuery(sqlSelect);
        System.out.println(resultSet);
    }

    @Test
    public void testDescTable() throws SQLException {
        TableService tableService = new TableService();
        tableService.reloadTable("students");
    }
}
