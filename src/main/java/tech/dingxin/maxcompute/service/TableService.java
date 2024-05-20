package tech.dingxin.maxcompute.service;

import com.aliyun.odps.type.TypeInfo;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;
import tech.dingxin.maxcompute.entity.SqlLiteColumn;
import tech.dingxin.maxcompute.utils.CommonUtils;
import tech.dingxin.maxcompute.utils.SqlRunner;
import tech.dingxin.maxcompute.utils.TypeConvertUtils;

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
    public String reloadTable(String tableName) {
        JsonObject table = new JsonObject();
        table.add("lastDDLTime", new JsonPrimitive(System.currentTimeMillis() / 1000));

        JsonArray columns = new JsonArray();
        JsonArray primaryKey = new JsonArray();

        List<SqlLiteColumn> schema = getSchema(tableName);
        for (SqlLiteColumn column : schema) {
            columns.add(toJson(column.getName(), TypeConvertUtils.convertToMaxComputeType(column.getType()),
                    column.isNotNull(), column.getDefaultValue()));
            if (column.isPrimaryKey()) {
                primaryKey.add(new JsonPrimitive(column.getName()));
            }
        }
        table.add("columns", columns);

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

    public List<SqlLiteColumn> getSchema(String tableName) {
        if (StringUtils.isNotEmpty(tableName)) {
            return SqlRunner.getSchema(tableName);
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
