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

package com.aliyun.odps.service;

import com.aliyun.odps.entity.PlanSplitRequest;
import com.aliyun.odps.entity.PlanSplitResponse;
import com.aliyun.odps.entity.SqlLiteColumn;
import com.aliyun.odps.utils.CommonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Service
public class StorageService {
    private Map<String, String> sessionIdTableMap;
    @Autowired
    private TableService tableService;

    public StorageService() {
        this.sessionIdTableMap = new HashMap<>();
    }

    public PlanSplitResponse planSplit(PlanSplitRequest request) {
        try {
            String sessionId = CommonUtils.generateUUID();
            sessionIdTableMap.put(sessionId, request.getTable());
            long rowCount = tableService.getRowCount(request.getTable());
            List<SqlLiteColumn> schema = tableService.getSchema(request.getTable());
            return new PlanSplitResponse(sessionId, null, null, schema, rowCount, 1);
        } catch (Exception e) {
            e.printStackTrace();
            return new PlanSplitResponse(null, null, e.getMessage(), null, 0, 0);
        }
    }

    public void readTable(String table, String sessionId, int maxBatchRows, int splitIndex, OutputStream outputStream) {
        // TODO
        //        try (ArrowStreamWriter arrowWriter = new ArrowStreamWriter(vectorSchemaRoot, null, outputStream)) {
        //            arrowWriter.start();
        //            // Your logic to write data to VectorSchemaRoot goes here.
        //
        //            // For example, write a batch of data, you might loop this based on your actual data source.
        //            arrowWriter.writeBatch();
        //
        //            // Finalize and close the writer.
        //            arrowWriter.end();
        //        } finally {
        //            vectorSchemaRoot.close();
        //        }
    }

//    private List<ArrowRowData> read(String tableName) throws Exception {
//        try (
//                Connection conn = CommonUtils.getConnection();
//                Statement stmt = conn.createStatement();
//                ResultSet rs = stmt.executeQuery(
//                        "select * from " + tableName.toUpperCase() + ";")
//        ) {
//            return CommonUtils.convertToRowData(rs);
//        }
//    }
}
