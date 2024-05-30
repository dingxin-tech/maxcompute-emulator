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

import com.aliyun.odps.antlr4.generate.MaxComputeSQLBaseListener;
import com.aliyun.odps.antlr4.generate.MaxComputeSQLLexer;
import com.aliyun.odps.antlr4.generate.MaxComputeSQLParser;
import com.aliyun.odps.entity.SqlLiteColumn;
import com.aliyun.odps.entity.SqlLiteSchema;
import com.aliyun.odps.utils.SqlRunner;
import com.aliyun.odps.utils.TypeConvertUtils;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CreateTable {
    public static void execute(String sql) throws SQLException {
        if (!sql.startsWith("CREATE TABLE") && !sql.startsWith("create table")) {
            throw new IllegalArgumentException("sql must start with CREATE TABLE");
        }
        run(sql);
    }

    private static void run(String originSql) throws SQLException {
        CharStream input = CharStreams.fromString(originSql);
        MaxComputeSQLLexer lexer = new MaxComputeSQLLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        MaxComputeSQLParser parser = new MaxComputeSQLParser(tokens);
        ParseTree tree = parser.sqlStatement();

        ParseTreeWalker walker = new ParseTreeWalker();
        MaxComputeListener listener = new MaxComputeListener();
        walker.walk(listener, tree);

        SqlRunner.executeSql(listener.getResult());
        if (!Table.of("schemas").exist()) {
            SqlRunner.executeSql("CREATE TABLE schemas(table_name TEXT, schema TEXT);");
        }
        SqlRunner.executeSql(
                "INSERT INTO schemas VALUES ('" + listener.getTableName() + "', '" + listener.getSchema().toJson() +
                        "');");
    }

    static class MaxComputeListener extends MaxComputeSQLBaseListener {
        private boolean ifNotExist;
        private String tableName;
        private StringBuilder result;
        private List<SqlLiteColumn> dataColumns = new ArrayList<>();
        private List<SqlLiteColumn> partitionColumns = new ArrayList<>();
        private SqlLiteSchema schema = new SqlLiteSchema(dataColumns, partitionColumns);

        @Override
        public void enterTableName(MaxComputeSQLParser.TableNameContext ctx) {
            tableName = ctx.IDENTIFIER().get(ctx.IDENTIFIER().size() - 1).getText();
        }

        @Override
        public void enterColumnDef(MaxComputeSQLParser.ColumnDefContext ctx) {
            String columnId = ctx.quotedIdentifier().IDENTIFIER().getText();
            String columnType = TypeConvertUtils.convertToSqlLiteType(ctx.dataType().getText());
            boolean notNull = ctx.notNull() != null;
            dataColumns.add(new SqlLiteColumn(columnId, columnType, notNull, null, false, false));
        }

        @Override
        public void enterIfNotExists(MaxComputeSQLParser.IfNotExistsContext ctx) {
            ifNotExist = true;
        }

        @Override
        public void enterPrimaryKey(MaxComputeSQLParser.PrimaryKeyContext ctx) {
            for (int i = 0; i < ctx.quotedIdentifier().size(); i++) {
                String pkColumn = ctx.quotedIdentifier(i).IDENTIFIER().getText();
                dataColumns.stream().filter(c -> c.getName().equals(pkColumn)).findFirst().get().setPrimaryKey(true);
            }
        }

        @Override
        public void enterPartitionColumnDef(MaxComputeSQLParser.PartitionColumnDefContext ctx) {
            String columnId = ctx.quotedIdentifier().IDENTIFIER().getText();
            String columnType = ctx.dataType().getText();
            partitionColumns.add(new SqlLiteColumn(columnId, columnType, ctx.notNull() != null, null, false, true));
        }

        @Override
        public void exitCreateTable(MaxComputeSQLParser.CreateTableContext ctx) {
        }

        public String getResult() {
            List<String> primaryKey = new ArrayList<>();
            result = new StringBuilder();
            result.append("CREATE TABLE ")
                    .append(ifNotExist ? "IF NOT EXISTS " : "")
                    .append(tableName)
                    .append(" (");
            for (SqlLiteColumn column : dataColumns) {
                result.append(column.getName()).append(" ").append(column.getType());
                if (column.isNotNull()) {
                    result.append(" NOT NULL");
                }
                if (column.isPrimaryKey()) {
                    primaryKey.add(column.getName());
                }
                result.append(",");
            }
            result.deleteCharAt(result.length() - 1);
            for (SqlLiteColumn column : partitionColumns) {
                result.append(",").append(column.getName()).append(" ").append(column.getType());
                if (column.isNotNull()) {
                    result.append(" NOT NULL");
                }
            }

            result.append(", PRIMARY KEY(");
            for (String pk : primaryKey) {
                result.append(pk).append(",");
            }
            for (SqlLiteColumn column : partitionColumns) {
                result.append(column.getName()).append(",");
            }
            result.deleteCharAt(result.length() - 1);
            result.append(")");

            result.append(")");
            return result.toString();
        }

        public SqlLiteSchema getSchema() {
            return schema;
        }

        public String getTableName() {
            return tableName;
        }
    }

}
