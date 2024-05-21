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

import com.aliyun.odps.service.TableService;
import org.junit.jupiter.api.Test;

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
        String resultSet = SqlRunner.execute(sqlSelect);
        System.out.println(resultSet);
    }

    @Test
    public void testDescTable() throws SQLException {
        TableService tableService = new TableService();
        tableService.reloadTable("students");
    }

    @Test
    public void showCreateTable() throws SQLException {
        String sqlInsert = "show create table students;";
        System.out.println(SqlRunner.execute(sqlInsert));
    }
}
