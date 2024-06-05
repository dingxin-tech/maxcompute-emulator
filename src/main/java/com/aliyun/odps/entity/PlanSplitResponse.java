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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Data
@AllArgsConstructor
public class PlanSplitResponse {
    private String sessionId;
    private Long expirationTime;
    private String errorMessage;
    private List<SqlLiteColumn> readSchema;
    private Long recordCount;
    private Integer splitCount;

    public String toJson() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        JsonObject jsonRoot = new JsonObject();

        // Session ID
        if (sessionId != null) {
            jsonRoot.addProperty("SessionId", sessionId);
        }

        // Expiration Time
        if (expirationTime != null) {
            jsonRoot.addProperty("ExpirationTime", expirationTime);
        }

        // Session Type
        jsonRoot.addProperty("SessionType", "batch_read");

        // Session Status
        jsonRoot.addProperty("SessionStatus", "NORMAL");

        // Error Message
        if (errorMessage != null) {
            jsonRoot.addProperty("Message", errorMessage);
        }

        // Data Schema
        if (readSchema != null) {
            JsonObject dataSchemaJson = new JsonObject();
            JsonArray dataColumnsJson = new JsonArray();
            JsonArray partitionColumnsJson = new JsonArray();

            // DataColumns
            for (SqlLiteColumn column : readSchema) {
                // Assumes the existence of a method to convert Column to JsonObject
                dataColumnsJson.add(column.toOdpsJson());
            }
            dataSchemaJson.add("DataColumns", dataColumnsJson);

            // TODO: PartitionColumns
            dataSchemaJson.add("PartitionColumns", partitionColumnsJson);

            jsonRoot.add("DataSchema", dataSchemaJson);
        }

        // Record count and splits count (InputSplitAssigner)
        // This part requires a way to identify the type of inputSplitAssigner as well as methods to get the relevant values
        if (recordCount != null) {
            jsonRoot.addProperty("RecordCount", recordCount);
        }
        if (splitCount != null) {
            jsonRoot.addProperty("SplitsCount", splitCount);
        }

        return gson.toJson(jsonRoot);
    }
}
