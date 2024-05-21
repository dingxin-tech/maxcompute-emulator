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

import com.aliyun.odps.entity.internal.project.ProjectModel;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;

import static com.aliyun.odps.rest.SimpleXmlUtils.marshal;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@RestController
public class ProjectController {
    @GetMapping("/projects/{projectName}")
    public ResponseEntity getProject(@PathVariable("projectName") String projectName) throws Exception {
        ProjectModel projectModel = new ProjectModel();
        projectModel.setName(projectName);
        projectModel.setOwner("MaxCompute Simulator");

        LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        properties.put("odps.schema.model.enabled", "false");
        projectModel.setProperties(properties);

        HttpHeaders headers = new HttpHeaders();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US)
                .withZone(java.time.ZoneId.of("GMT"));
        String dateString = formatter.format(ZonedDateTime.now());
        headers.set("x-odps-creation-time", dateString);
        headers.set("Last-Modified", dateString);
        headers.set("x-odps-owner", "MaxCompute Simulator");

        return new ResponseEntity<>(marshal(projectModel), headers, HttpStatus.OK);
    }

}
