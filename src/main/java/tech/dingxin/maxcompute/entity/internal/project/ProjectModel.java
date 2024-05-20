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

package tech.dingxin.maxcompute.entity.internal.project;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */

import com.aliyun.odps.StorageTierInfo;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.simpleframework.xml.convert.Converter;
import com.aliyun.odps.simpleframework.xml.stream.InputNode;
import com.aliyun.odps.simpleframework.xml.stream.OutputNode;
import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Project model (ignore Cluster info for mocked version)
 */
@Root(name = "Project", strict = false)
@Data
public class ProjectModel {

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Type", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String type;

    @Element(name = "Comment", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String comment;

    @Element(name = "Owner", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String owner;

    @Element(name = "SuperAdministrator", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String superAdministrator;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date creationTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModified;

    @Element(name = "ProjectGroupName", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String projectGroupName;

    @Element(name = "DefaultQuotaNickname", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String defaultQuotaNickname;

    @Element(name = "DefaultQuotaRegion", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String defaultQuotaRegion;

    @Element(name = "Properties", required = false)
    @Convert(PropertyConverter.class)
    LinkedHashMap<String, String> properties;

    @Element(name = "Property", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String propertyJsonString;

    @Element(name = "ExtendedProperties", required = false)
    @Convert(PropertyConverter.class)
    LinkedHashMap<String, String> extendedProperties;

    @Element(name = "State", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String state;

    @Element(name = "DefaultCluster", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String defaultCluster;

    @Element(name = "QuotaID", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String defaultQuotaID;

    @Element(name = "Region", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String regionId;

    @Element(name = "TenantId", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String tenantId;

    /**
     * 分层存储信息
     */
    StorageTierInfo storageTierInfo;

    @Root(name = "Property", strict = false)
    static class Property {

        Property() {
        }

        Property(String name, String value) {
            this.name = name;
            this.value = value;
        }

        @Element(name = "Name", required = false)
        @Convert(SimpleXmlUtils.EmptyStringConverter.class)
        String name;

        @Element(name = "Value", required = false)
        @Convert(SimpleXmlUtils.EmptyStringConverter.class)
        String value;
    }

    @Root(name = "Properties", strict = false)
    static class Properties {

        @ElementList(entry = "Property", inline = true, required = false)
        List<Property> entries = new ArrayList<>();
    }

    static class PropertyConverter implements Converter<LinkedHashMap<String, String>> {

        @Override
        public void write(OutputNode outputNode, LinkedHashMap<String, String> properties)
                throws Exception {
            if (properties != null) {
                for (Map.Entry<String, String> entry : properties.entrySet()) {
                    String name = entry.getKey();
                    String value = entry.getValue();
                    SimpleXmlUtils.marshal(new Property(name, value), outputNode);
                }
            }

            outputNode.commit();
        }

        @Override
        public LinkedHashMap<String, String> read(InputNode inputNode) throws Exception {
            LinkedHashMap<String, String> properties = new LinkedHashMap<String, String>();
            Properties props = SimpleXmlUtils.unmarshal(inputNode, Properties.class);
            for (Property entry : props.entries) {
                properties.put(entry.name, entry.value);
            }
            return properties;
        }
    }
}