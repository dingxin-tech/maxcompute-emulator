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

package tech.dingxin.maxcompute.entity.internal.instance;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Attribute;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.Text;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import org.apache.commons.codec.binary.Base64;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Root(name = "Instance", strict = false)
public class InstanceResultModel {

    @Root(strict = false)
    public static class TaskResult {

        @Attribute(name = "Type", required = false)
        String type;

        @Element(name = "Name", required = false)
        @Convert(SimpleXmlUtils.EmptyStringConverter.class)
        String name;

        @Element(name = "Result", required = false)
        Result result;

        @Element(name = "Status", required = false)
        @Convert(SimpleXmlUtils.EmptyStringConverter.class)
        String status;

        public String getType() {
            return type;
        }

        public String getName() {
            return name;
        }

        public Result getResult() {
            return result;
        }

        public String getStatus() {
            return status;
        }
    }

    @ElementList(name = "Tasks", entry = "Task", required = false)
    List<TaskResult> taskResults = new ArrayList<>();

    @Root(strict = false)
    public static class Result {

        @Attribute(name = "Transform", required = false)
        String transform;

        @Attribute(name = "Format", required = false)
        String format;

        @Text(required = false)
        String text = "";

        /**
         * {@link Instance}执行后，无论成功与否，返回给客户端的结果或失败信息。
         */
        public String getString() {
            if (transform != null && "Base64".equals(transform)) {
                try {
                    String decodedString = new String(Base64.decodeBase64(text), "UTF-8");
                    return decodedString;
                } catch (Exception e) {
                    // return original text
                    return text;
                }
            } else {
                return text;
            }
        }

        /**
         * 指明返回结果的格式，包括：text, csv
         *
         * @return 格式信息包括："text"，"csv"
         */
        public String getFormat() {
            return format;
        }
    }

    public void addTaskResult(String taskName, String result) {
        TaskResult taskResult = new TaskResult();
        taskResult.type = "SQL";
        taskResult.name = taskName;
        taskResult.result = new Result();
        taskResult.result.format = "csv";
        taskResult.result.text = result;
        taskResult.status = "Success";
        taskResults.add(taskResult);
    }

}