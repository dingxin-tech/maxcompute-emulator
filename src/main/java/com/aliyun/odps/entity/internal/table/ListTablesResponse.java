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

package com.aliyun.odps.entity.internal.table;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Root(name = "Tables", strict = false)
public class ListTablesResponse {

    @ElementList(entry = "Table", inline = true, required = false)
    private List<TableModel> tables = new ArrayList<>();

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String marker;

    @Element(name = "MaxItems", required = false)
    private Integer maxItems;

    public ListTablesResponse(List<String> names) {
        names.forEach(name -> tables.add(new TableModel(name, "")));
        marker = "";
        maxItems = names.size();
    }
}