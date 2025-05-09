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

import com.aliyun.odps.Job;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.common.MessageResponse;
import com.aliyun.odps.entity.ErrorMessage;
import com.aliyun.odps.entity.SQLResult;
import com.aliyun.odps.entity.internal.instance.Instance;
import com.aliyun.odps.entity.internal.instance.InstanceResultModel;
import com.aliyun.odps.entity.internal.instance.InstanceStatusModel;
import com.aliyun.odps.entity.internal.instance.SQL;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.CommonUtils;
import com.aliyun.odps.utils.XmlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.sql.SQLException;
import java.sql.Time;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.aliyun.odps.rest.SimpleXmlUtils.marshal;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@RestController
public class InstanceController {
    private static final Logger LOG = LoggerFactory.getLogger(InstanceController.class);

    Map<String, Map<String, SQLResult>> instanceResultMap;

    Map<String, Instant> instanceRunningMap;

    private static final int DEFAULT_RUNNING_TIME = 60;

    public InstanceController() {
        instanceResultMap = new HashMap<>();
        instanceRunningMap = new HashMap<>();
    }

    @PostMapping("/projects/{projectName}/instances")
    @ResponseBody
    public ResponseEntity<Object> createInstance(
            @PathVariable("projectName") String projectName,
            @RequestParam("curr_project") String currProject,
            @RequestBody String body) throws SQLException {
        try {

            Instance instance = XmlUtils.parseInstance(body);
            SQL sql = instance.getJob().getTasks().getSql();
            String name = sql.getName();
            String query = sql.getQuery().toUpperCase().trim();
            query = query.replaceAll("\\s+", " ");
            String instanceId = CommonUtils.generateUUID();
            //LOG.info("create instance {} to execute query {}", instanceId, query);

            // just return any result
            String result = "a, b \r\n c, d \r\n";
            //LOG.info("instance {} result {}", instanceId, result);
            instanceResultMap.putIfAbsent(instanceId, new HashMap<>());
            instanceResultMap.get(instanceId).put(name, new SQLResult(query, result));
            instanceRunningMap.put(instanceId, Instant.now().plus(DEFAULT_RUNNING_TIME, TimeUnit.SECONDS.toChronoUnit()));

            HttpHeaders headers = new HttpHeaders();
            headers.setLocation(URI.create("/" + instanceId));
            return new ResponseEntity<>(new MessageResponse("Created"), headers, HttpStatus.CREATED);
        } catch (Exception e) {
            LOG.error("create instance error", e);
            return new ResponseEntity<>(ErrorMessage.of(e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/projects/{projectName}/instances/{instanceId}")
    @ResponseBody
    public ResponseEntity<Object> getInstance(@PathVariable("projectName") String projectName,
                                              @PathVariable("instanceId") String instanceId,
                                              @RequestParam("curr_project") String currProject) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US)
                .withZone(java.time.ZoneId.of("GMT"));

        String dateString = formatter.format(ZonedDateTime.now());
        headers.set("x-odps-start-time", dateString);
        headers.set("x-odps-end-time", dateString);
        headers.set("x-odps-request-id", instanceId);
        headers.set("x-odps-owner", "MaxCompute Simulator");

        Instant endTime = instanceRunningMap.get(instanceId);
        if (endTime.isAfter(Instant.now())) {
            return new ResponseEntity<>(marshal(new InstanceStatusModel("Running")), headers, HttpStatus.OK);
        } else {
            return new ResponseEntity<>(marshal(new InstanceStatusModel()), headers, HttpStatus.OK);
        }
    }

    @GetMapping(value = "/projects/{projectName}/instances/{instanceId}", params = "instancestatus")
    @ResponseBody
    public  ResponseEntity<Object> checkInstanceStatusBlock(@PathVariable("projectName") String projectName,
                                      @PathVariable("instanceId") String instanceId,
                                      @RequestParam("curr_project") String currProject) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US)
                .withZone(java.time.ZoneId.of("GMT"));

        String dateString = formatter.format(ZonedDateTime.now());
        headers.set("x-odps-start-time", dateString);
        headers.set("x-odps-end-time", dateString);
        headers.set("x-odps-request-id", instanceId);
        headers.set("x-odps-owner", "MaxCompute Simulator");

        Instant endTime = instanceRunningMap.get(instanceId);
        long timeToStop = endTime.getEpochSecond() - Instant.now().getEpochSecond();
        if (timeToStop > 0) {
            if (timeToStop < 5) {
                TimeUnit.SECONDS.sleep(timeToStop);
                return new ResponseEntity<>(marshal(new InstanceStatusModel()), headers, HttpStatus.OK);
            } else {
                TimeUnit.SECONDS.sleep(5);
            }
            return new ResponseEntity<>(marshal(new InstanceStatusModel("Running")), headers, HttpStatus.OK);
        } else {
            return new ResponseEntity<>(marshal(new InstanceStatusModel()), headers, HttpStatus.OK);
        }
    }

    @GetMapping(value = "/projects/{projectName}/instances/{instanceId}", params = "taskstatus")
    @ResponseBody
    public String checkInstanceStatus(@PathVariable("projectName") String projectName,
                                      @PathVariable("instanceId") String instanceId,
                                      @RequestParam("curr_project") String currProject) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<Instance><Status>Terminated</Status><Tasks><Task Type=\"SQL\"><Name>sqlrt_fallback_task</Name><StartTime>Sat, " +
                    "11 May 2024 02:08:18 GMT</StartTime><EndTime>Sat, 11 May 2024 02:08:29 GMT</EndTime><Status>Success</Status><Histories/></Task></Tasks></Instance>";
    }


    @GetMapping(value = "/projects/{projectName}/instances/{instanceId}", params = "source")
    @ResponseBody
    public String getJob(@PathVariable("projectName") String projectName,
                         @PathVariable("instanceId") String instanceId) throws OdpsException {
        Job job = new Job();
        Map<String, SQLResult> results = instanceResultMap.get(instanceId);
        if (results == null) {
            return job.toXmlString();
        }
        results.entrySet().forEach(entry -> {
            SQLTask sqlTask = new SQLTask();
            sqlTask.setName(entry.getKey());
            sqlTask.setQuery(entry.getValue().getQuery());
            job.addTask(sqlTask);
        });
        return job.toXmlString();
    }

    @GetMapping(value = "/projects/{projectName}/instances/{instanceId}", params = "result")
    @ResponseBody
    public String getInstanceResult(@PathVariable("projectName") String projectName,
                                    @PathVariable("instanceId") String instanceId,
                                    @RequestParam("curr_project") String currProject) throws Exception {
        InstanceResultModel instanceResultModel = new InstanceResultModel();
        Map<String, SQLResult> results = instanceResultMap.get(instanceId);
        results.entrySet()
                .forEach(entry -> instanceResultModel.addTaskResult(entry.getKey(), entry.getValue().getResult()));
        String marshal = marshal(instanceResultModel);
        return marshal;
    }

    @PostMapping("/projects/{projectName}/authorization")
    public String generateLogView() throws Exception {
        return marshal(new AuthorizationQueryResponse());
    }

    @Root(name = "Authorization", strict = false)
    static class AuthorizationQueryResponse {
        @Element(name = "Result", required = false)
        @Convert(SimpleXmlUtils.EmptyStringConverter.class)
        String result;
    }
}
