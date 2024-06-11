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

import com.aliyun.odps.common.Options;
import com.aliyun.odps.entity.ErrorMessage;
import com.aliyun.odps.entity.RowData;
import com.aliyun.odps.entity.SqlLiteColumn;
import com.aliyun.odps.entity.TableId;
import com.aliyun.odps.function.Table;
import com.aliyun.odps.function.UpsertTable;
import com.aliyun.odps.service.TableService;
import com.aliyun.odps.utils.CommonUtils;
import com.aliyun.odps.utils.Deserializer;
import com.aliyun.odps.utils.TypeConvertUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@RestController
public class TunnelController {
    private static final Logger LOG = LoggerFactory.getLogger(TunnelController.class);

    private Map<String, TableId> upsertSessionMap;

    @Autowired
    private TableService tableService;

    public TunnelController() {
        upsertSessionMap = new HashMap<>();
    }

    @PostMapping("/projects/{projectName}/tables/{tableId}/upserts")
    @ResponseBody
    public ResponseEntity<String> createOrCommitUpsertSession(
            @PathVariable("tableId") String tableId,
            @RequestParam(value = "partition", required = false, defaultValue = "") String partition,
            @RequestParam(value = "upsertid", required = false, defaultValue = "") String upsertId
    ) {
        if (!tableService.tableExist(tableId)) {
            return new ResponseEntity<>("table not exist", HttpStatus.NOT_FOUND);
        }
        try {
            JsonObject result = new JsonObject();
            boolean commit = false;
            // sessionId
            if (upsertId.isEmpty()) {
                upsertId = CommonUtils.generateUUID();
                upsertSessionMap.put(upsertId, TableId.of(tableId, partition));
                LOG.info("create upsert session {} for table {}", upsertId, tableId);
            } else {
                upsertSessionMap.remove(upsertId);
                commit = true;
                LOG.info("commit upsert session {} ", upsertId, tableId);
            }

            result.add("id", new JsonPrimitive(upsertId));
            // tunnelTableSchema
            JsonObject schema = new JsonObject();
            JsonArray columns = new JsonArray();
            JsonArray hashKeys = new JsonArray();

            List<SqlLiteColumn> sqlLiteSchema = tableService.getDataSchema(tableId.toUpperCase());
            for (int cid = 0; cid < sqlLiteSchema.size(); cid++) {
                SqlLiteColumn column = sqlLiteSchema.get(cid);
                JsonObject columnJson = new JsonObject();
                columnJson.add("name", new JsonPrimitive(column.getName()));
                columnJson.add("type",
                        new JsonPrimitive(TypeConvertUtils.convertToMaxComputeType(column.getType()).getTypeName()));
                columnJson.add("nullable", new JsonPrimitive(column.isNotNull()));
                columnJson.add("column_id", new JsonPrimitive(cid));
                columns.add(columnJson);
                if (column.isPrimaryKey()) {
                    hashKeys.add(new JsonPrimitive(column.getName()));
                }
            }
            schema.add("columns", columns);

            JsonArray partitionColumns = new JsonArray();
            List<SqlLiteColumn> sqlLitePartitionSchema = tableService.getPartitionSchema(tableId.toUpperCase());
            for (int cid = 0; cid < sqlLitePartitionSchema.size(); cid++) {
                SqlLiteColumn column = sqlLitePartitionSchema.get(cid);
                JsonObject columnJson = new JsonObject();
                columnJson.add("name", new JsonPrimitive(column.getName()));
                columnJson.add("type",
                        new JsonPrimitive(TypeConvertUtils.convertToMaxComputeType(column.getType()).getTypeName()));
                columnJson.add("nullable", new JsonPrimitive(column.isNotNull()));
                columnJson.add("column_id", new JsonPrimitive(cid));
                partitionColumns.add(columnJson);
            }
            schema.add("partitionKeys", partitionColumns);
            result.add("schema", schema);

            // hash_key
            result.add("hash_key", hashKeys);

            // hasher
            result.add("hasher", new JsonPrimitive("default"));

            // slots
            JsonArray slots = new JsonArray();
            JsonObject slot = new JsonObject();
            slot.add("slot_id", new JsonPrimitive(0));
            JsonArray buckets = new JsonArray();
            buckets.add(0);
            slot.add("buckets", buckets);
            slot.add("worker_addr", new JsonPrimitive(Options.ENDPOINT));
            slots.add(slot);

            result.add("slots", slots);
            if (commit) {
                result.add("status", new JsonPrimitive("committed"));
            } else {
                result.add("status", new JsonPrimitive("normal"));
            }

            HttpHeaders headers = new HttpHeaders();
            headers.set("x-odps-request-id", upsertId);
            return new ResponseEntity<>(result.toString(), headers, HttpStatus.OK);
        } catch (Exception e) {
            LOG.error("create upsert session error", e);
            HttpHeaders headers = new HttpHeaders();
            headers.set("x-odps-request-id", upsertId);
            return new ResponseEntity(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/projects/{projectName}/tables/{tableId}/upserts")
    @ResponseBody
    public ResponseEntity reloadUpsertSession(@RequestParam("upsertid") String sessionId) {
        try {
            JsonObject result = new JsonObject();
            // sessionId

            TableId tableId = upsertSessionMap.getOrDefault(sessionId, new TableId());

            result.add("id", new JsonPrimitive(sessionId));
            // tunnelTableSchema
            JsonObject schema = new JsonObject();
            JsonArray columns = new JsonArray();
            JsonArray hashKeys = new JsonArray();

            List<SqlLiteColumn> sqlLiteSchema = tableService.getDataSchema(tableId.getTableName());
            for (int cid = 0; cid < sqlLiteSchema.size(); cid++) {
                SqlLiteColumn column = sqlLiteSchema.get(cid);
                if (column.isPartitionKey()) {
                    continue;
                }
                JsonObject columnJson = new JsonObject();
                columnJson.add("name", new JsonPrimitive(column.getName()));
                columnJson.add("type",
                        new JsonPrimitive(TypeConvertUtils.convertToMaxComputeType(column.getType()).getTypeName()));
                columnJson.add("nullable", new JsonPrimitive(column.isNotNull()));
                columnJson.add("column_id", new JsonPrimitive(cid));
                columns.add(columnJson);
                if (column.isPrimaryKey()) {
                    hashKeys.add(new JsonPrimitive(column.getName()));
                }
            }
            schema.add("columns", columns);
            //TODO: schema.add("partitionKeys", columns);
            result.add("schema", schema);

            // hash_key
            result.add("hash_key", hashKeys);

            // hasher
            result.add("hasher", new JsonPrimitive("default"));

            // slots
            JsonArray slots = new JsonArray();
            JsonObject slot = new JsonObject();
            slot.add("slot_id", new JsonPrimitive(0));
            JsonArray buckets = new JsonArray();
            buckets.add(0);
            slot.add("buckets", buckets);
            slot.add("worker_addr", new JsonPrimitive(Options.ENDPOINT));
            slots.add(slot);

            result.add("slots", slots);
            result.add("status", new JsonPrimitive(tableId.getTableName() == null ? "committed" : "normal"));
            HttpHeaders headers = new HttpHeaders();
            headers.set("x-odps-request-id", sessionId);
            return new ResponseEntity<>(result.toString(), headers, HttpStatus.OK);
        } catch (Exception e) {
            LOG.error("create upsert session error", e);
            HttpHeaders headers = new HttpHeaders();
            headers.set("x-odps-request-id", sessionId);
            return new ResponseEntity(ErrorMessage.of(e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/projects/{projectName}/tables/{tableId}/upserts")
    @ResponseBody
    public ResponseEntity flushData(
            @PathVariable("tableId") String tableId,
            @RequestParam(value = "partition", required = false, defaultValue = "") String partition,
            @RequestParam(value = "upsertid", required = true, defaultValue = "") String sessionId,
            @RequestHeader(value = "Content-Encoding", required = false, defaultValue = "") String compression,
            @RequestBody byte[] requestBody
    ) throws IOException {
        if (!upsertSessionMap.containsKey(sessionId)) {
            return new ResponseEntity("session has been committed", HttpStatus.NOT_FOUND);
        }
        try {
            List<RowData> records = Deserializer.deserializeData(new ByteArrayInputStream(requestBody),
                    tableService.getDataSchema(tableId.toUpperCase()).stream()
                            .map(c -> TypeConvertUtils.convertToMaxComputeType(c.getType()))
                            .collect(Collectors.toList()));
            UpsertTable.upsertData(tableId, records, Table.of(tableId.toUpperCase()).getSchema(), partition);
            HttpHeaders headers = new HttpHeaders();
            headers.set("x-odps-request-id", sessionId);
            return new ResponseEntity<>("OK", headers, HttpStatus.OK);
        } catch (Exception e) {
            LOG.error("flush data error", e);
            return new ResponseEntity(ErrorMessage.of(e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    @GetMapping("/projects/{projectName}/tunnel")
    @ResponseBody
    public String getTunnelEndpoint() {
        return Options.ENDPOINT;
    }
}
