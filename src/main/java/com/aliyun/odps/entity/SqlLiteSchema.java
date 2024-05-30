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

package com.aliyun.odps.entity;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SqlLiteSchema {
    List<SqlLiteColumn> columns;
    List<SqlLiteColumn> partitionColumns;

    public List<SqlLiteColumn> getColumns() {
        return new ArrayList<>(columns);
    }

    public List<SqlLiteColumn> getPartitionColumns() {
        return new ArrayList<>(partitionColumns);
    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public static SqlLiteSchema fromJson(String json) {
        Gson gson = new Gson();
        return gson.fromJson(json, SqlLiteSchema.class);
    }
}
