package tech.dingxin.maxcompute.service;

import com.aliyun.odps.type.TypeInfo;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.springframework.stereotype.Service;
import tech.dingxin.maxcompute.utils.CommonUtils;
import tech.dingxin.maxcompute.utils.TypeConvertUtils;

import java.sql.Connection;
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
    public String reloadTable(String tableName) {
        JsonObject table = new JsonObject();
        table.add("lastDDLTime", new JsonPrimitive(System.currentTimeMillis() / 1000));

        JsonArray columns = new JsonArray();
        JsonArray primaryKey = new JsonArray();
        try (Statement stmt = CommonUtils.getConnection().createStatement()) {
            ResultSet rs = stmt.executeQuery("PRAGMA table_info('" + tableName + "')");

            while (rs.next()) {
                String name = rs.getString("name");
                String type = rs.getString("type");
                boolean notnull = rs.getBoolean("notnull");
                String dfltValue = rs.getString("dflt_value");
                boolean pk = rs.getBoolean("pk");
                columns.add(toJson(name, TypeConvertUtils.convertToMaxComputeType(type), notnull, dfltValue));
                if (pk) {
                    primaryKey.add(new JsonPrimitive(name));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        table.add("columns", columns);

        // reverse info
        JsonObject reverseInfo = new JsonObject();
        reverseInfo.add("Transactional", new JsonPrimitive(true));
        reverseInfo.add("PrimaryKey", primaryKey);
        table.add("Reserved", new JsonPrimitive(reverseInfo.toString()));

        return table.toString();
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
