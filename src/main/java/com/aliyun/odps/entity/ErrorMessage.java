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

import com.aliyun.odps.common.Options;
import com.aliyun.odps.utils.GsonObjectBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ErrorMessage {
    @Expose
    @SerializedName("Code")
    private String errorcode;

    @Expose
    @SerializedName("Message")
    private String message;

    @Expose
    @SerializedName("RequestId")
    private String requestId;
    public String HostId;

    public ErrorMessage(String errorMessage) {
        this.errorcode = "EMULATOR_MOCKED_ERROR";
        this.message = errorMessage;
        this.requestId = "mocked_request_id";
        this.HostId = Options.ENDPOINT;
    }

    public static String of(String errorMessage) {
        return new ErrorMessage(errorMessage).toJson();
    }

    public String getErrorcode() {
        return this.errorcode;
    }

    public String getMessage() {
        return this.message;
    }

    public String getRequestId() {
        return this.requestId;
    }

    public String getHostId() {
        return this.HostId;
    }

    public String toJson() {
        return GsonObjectBuilder.get().toJson(this);
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("RequestId=").append(this.requestId).append(',');
        sb.append("Code=").append(this.errorcode).append(',');
        sb.append("Message=").append(this.message);
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println(ErrorMessage.of("errorMessage"));
    }
}