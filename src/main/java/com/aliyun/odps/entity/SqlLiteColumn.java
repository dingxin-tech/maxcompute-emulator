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

import com.aliyun.odps.utils.TypeConvertUtils;
import com.google.gson.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Data
@AllArgsConstructor
public class SqlLiteColumn {
    String name;
    String type;
    boolean notNull;
    String defaultValue;
    boolean primaryKey;
    boolean partitionKey;

    public JsonObject toOdpsJson() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("Name", name);
        jsonObject.addProperty("Type", TypeConvertUtils.convertToMaxComputeType(type).getTypeName());
        jsonObject.addProperty("Nullable", notNull);
        return jsonObject;
    }
}
