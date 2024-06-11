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

package com.aliyun.odps.controller;

import com.aliyun.odps.entity.PlanSplitRequest;
import com.aliyun.odps.entity.PlanSplitResponse;
import com.aliyun.odps.entity.TableId;
import com.aliyun.odps.service.StorageService;
import com.aliyun.odps.utils.CommonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.util.HashMap;
import java.util.Map;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@RestController
public class StorageReadController {

    @Autowired
    private StorageService storageService;

    @PostMapping(value = "/api/storage/v1/projects/{project}/schemas/{schema}/tables/{table}/sessions", params = "session_type=batch_read")
    public ResponseEntity<String> planInputSplit(@PathVariable("project") String project,
                                                 @PathVariable("schema") String schema,
                                                 @PathVariable("table") String table,
                                                 @RequestBody String jsonString) {
        PlanSplitRequest planSplitRequest = new PlanSplitRequest(table, jsonString);
        PlanSplitResponse response = storageService.planSplit(planSplitRequest);
        if (response.getErrorMessage() != null) {
            return ResponseEntity.internalServerError().body(response.toJson());
        }
        return ResponseEntity.ok(response.toJson());
    }

    @GetMapping("/api/storage/v1/projects/{project}/schemas/{schema}/tables/{table}/data")
    public ResponseEntity<StreamingResponseBody> readTable(@PathVariable("project") String project,
                                                           @PathVariable("schema") String schema,
                                                           @PathVariable("table") String table,
                                                           @RequestParam("session_id") String sessionId,
                                                           @RequestParam(value = "max_batch_rows", required = false)
                                                           Long maxBatchRows,
                                                           @RequestParam(value = "split_index", required = false)
                                                           Integer splitIndex) {
        StreamingResponseBody responseBody = outputStream -> {
            try {
                storageService.readTable(table, sessionId, maxBatchRows, splitIndex, outputStream);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        return ResponseEntity.ok()
                .header("Content-Type", "application/octet-stream")
                .body(responseBody);
    }
}
