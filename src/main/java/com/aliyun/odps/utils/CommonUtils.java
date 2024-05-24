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

import org.apache.arrow.vector.types.pojo.Schema;
import tech.dingxin.ArrowRowData;
import tech.dingxin.jdbc.JdbcUtils;
import tech.dingxin.jdbc.dialects.SqlLiteDialect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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

    public static List<ArrowRowData> convertToRowData(ResultSet resultSet) throws Exception {
        List<ArrowRowData> rowData = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        Schema schema = JdbcUtils.toArrowSchema(metaData, SqlLiteDialect.INSTANCE);

        while (resultSet.next()) {
            ArrowRowData row = new ArrowRowData(schema);
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                System.out.println(resultSet.getObject(i));
                row.set(i - 1, resultSet.getObject(i));
            }
            rowData.add(row);
        }
        return rowData;
    }
}
