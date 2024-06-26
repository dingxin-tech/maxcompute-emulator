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

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Data
@EqualsAndHashCode
public class TableId {
    private String tableName;
    private String partitionName;

    private List<String> partitionNames;

    public static TableId of(String tableName, String partitionName) {
        TableId tableId = new TableId();
        tableId.tableName = tableName;
        tableId.partitionName = partitionName;
        return tableId;
    }

    public static TableId of(String tableName, List<String> partitionNames) {
        TableId tableId = new TableId();
        tableId.tableName = tableName;
        tableId.partitionNames = partitionNames;
        return tableId;
    }
}
