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

import com.aliyun.odps.Table;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.Text;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import lombok.Data;

import java.util.Date;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Root(name = "Table", strict = false)
@Data
public class TableModel {

    @Root(name = "Schema", strict = false)
    static class Schema {
        @Text(required = false)
        String content;
    }

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "Schema", required = false)
    private Schema schema;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date createdTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModifiedTime;

    @Element(name = "LastAccessTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastAccessTime;

    @Element(name = "Type", required = false)
    @Convert(Table.TableTypeConverter.class)
    Table.TableType type;

    public TableModel(String name, String schema) {
        this.name = name;
        this.schema = new Schema();
        this.schema.content = schema;
        this.createdTime = new Date();
        this.lastModifiedTime = new Date();
        this.lastAccessTime = new Date();
        this.owner = "MaxCompute Emulator";
        this.type = Table.TableType.MANAGED_TABLE;
    }
}


