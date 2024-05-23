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
import com.google.gson.JsonPrimitive;
import lombok.Data;

import java.util.List;
import java.util.stream.StreamSupport;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Data
public class PlanSplitRequest {
    private String table;
    private List<String> requiredDataColumns;
    private List<String> requiredPartitionColumns;
    private List<String> requiredPartitions;
    private List<Integer> requiredBucketIds;
    private String splitMode;
    private int splitNumber;
    private boolean crossPartition;
    private int splitMaxFileNum;
    private String timestampUnit;
    private String datetimeUnit;
    private String filterPredicate;

    public PlanSplitRequest(String table, String jsonString) {
        this.table = table;
        Gson gson = new GsonBuilder().create();
        JsonObject request = gson.fromJson(jsonString, JsonObject.class);

        // Extract "RequiredDataColumns"
        requiredDataColumns = extractJsonArrayAsStringList(request.getAsJsonArray("RequiredDataColumns"));

        // Extract "RequiredPartitionColumns"
        requiredPartitionColumns =
                extractJsonArrayAsStringList(request.getAsJsonArray("RequiredPartitionColumns"));

        // Extract "RequiredPartitions"
        requiredPartitions = extractJsonArrayAsStringList(request.getAsJsonArray("RequiredPartitions"));

        // Extract "RequiredBucketIds"
        requiredBucketIds = extractJsonArrayAsIntegerList(request.getAsJsonArray("RequiredBucketIds"));

        // Extract "SplitOptions"
        JsonObject jsonSplitOptions = request.getAsJsonObject("SplitOptions");
        splitMode = jsonSplitOptions.get("SplitMode").getAsString();
        splitNumber = jsonSplitOptions.get("SplitNumber").getAsInt();
        crossPartition = jsonSplitOptions.get("CrossPartition").getAsBoolean();

        // Extract "SplitMaxFileNum"
        splitMaxFileNum = request.get("SplitMaxFileNum").getAsInt();

        // Extract "ArrowOptions"
        JsonObject jsonArrowOptions = request.getAsJsonObject("ArrowOptions");
        timestampUnit = jsonArrowOptions.get("TimestampUnit").getAsString();
        datetimeUnit = jsonArrowOptions.get("DatetimeUnit").getAsString();

        // Extract "FilterPredicate"
        filterPredicate = request.get("FilterPredicate").getAsString();
    }

    private List<String> extractJsonArrayAsStringList(JsonArray jsonArray) {
        return StreamSupport.stream(jsonArray.spliterator(), false)
                .map(JsonPrimitive.class::cast)
                .map(JsonPrimitive::getAsString)
                .toList();
    }

    private List<Integer> extractJsonArrayAsIntegerList(JsonArray jsonArray) {
        return StreamSupport.stream(jsonArray.spliterator(), false)
                .map(JsonPrimitive.class::cast)
                .map(JsonPrimitive::getAsInt)
                .toList();
    }
}
