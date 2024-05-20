package tech.dingxin.maxcompute.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CommonUtils {
    public static String generateUUID() {
        return java.util.UUID.randomUUID().toString().replace("-", "");
    }

    private static final String URL = "jdbc:sqlite:/tmp/maxcompute-emulator.db";

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL);
    }
}
