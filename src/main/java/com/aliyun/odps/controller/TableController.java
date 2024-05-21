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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.aliyun.odps.entity.internal.table.ListTablesResponse;
import com.aliyun.odps.entity.internal.table.TableModel;
import com.aliyun.odps.service.TableService;

import static com.aliyun.odps.rest.SimpleXmlUtils.marshal;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@RestController
public class TableController {
    @Autowired
    private TableService tableService;

    @GetMapping("/projects/{projectName}/tables/{tableId}")
    @ResponseBody
    public ResponseEntity<String> getTable(@PathVariable("projectName") String projectName,
                                           @PathVariable("tableId") String tableId) throws Exception {
        if (!tableService.tableExist(tableId)) {
            return new ResponseEntity<>("table " + tableId + " not found", null, HttpStatus.NOT_FOUND);
        }
        String responce = marshal(new TableModel(tableId, tableService.reloadTable(tableId)));
        return ResponseEntity.ok(responce);
    }

    @GetMapping("/projects/{projectName}/schemas/{schemaName}/tables/{tableId}")
    @ResponseBody
    public ResponseEntity<String> getTable(@PathVariable("projectName") String projectName,
                                           @PathVariable("schemaName") String schemaName,
                                           @PathVariable("tableId") String tableId) throws Exception {
        if (!tableService.tableExist(tableId)) {
            return new ResponseEntity<>("table " + tableId + " not found", null, HttpStatus.NOT_FOUND);
        }
        String responce = marshal(new TableModel(tableId, tableService.reloadTable(tableId)));
        return ResponseEntity.ok(responce);
    }

    @GetMapping("/projects/{projectName}/tables")
    @ResponseBody
    public ResponseEntity<String> listTable(@PathVariable("projectName") String projectName,
                                            @RequestParam("expectmarker") boolean expectmarker) throws Exception {
        String responce = marshal(new ListTablesResponse(tableService.listTables()));
        return ResponseEntity.ok(responce);
    }
}
