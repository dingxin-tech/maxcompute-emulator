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

package com.aliyun.odps.function;

import com.aliyun.odps.entity.SqlLiteSchema;
import com.aliyun.odps.utils.CommonUtils;
import com.aliyun.odps.utils.SqlRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class Table {
    private static final Logger LOG = LoggerFactory.getLogger(Table.class);
    private Boolean isPartitioned;
    private String tableName;

    public Table(String tableName) {
        this.tableName = tableName;
    }

    public static Table of(String tableName) {
        return new Table(tableName);
    }

    public boolean exist() {
        return isExist(tableName);
    }

    public SqlLiteSchema getSchema() throws SQLException {
        return SqlRunner.getSchema(tableName);
    }

    public static boolean isExist(String tableName) {
        try {
            final String query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
            try (Connection conn = CommonUtils.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setString(1, tableName);

                try (ResultSet resultSet = pstmt.executeQuery()) {
                    return resultSet.next();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

}
