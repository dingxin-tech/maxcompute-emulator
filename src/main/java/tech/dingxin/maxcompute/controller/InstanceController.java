package tech.dingxin.maxcompute.controller;

import com.aliyun.odps.Job;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.task.SQLTask;
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
import tech.dingxin.maxcompute.common.MessageResponse;
import tech.dingxin.maxcompute.entity.SQLResult;
import tech.dingxin.maxcompute.entity.internal.instance.Instance;
import tech.dingxin.maxcompute.entity.internal.instance.InstanceResultModel;
import tech.dingxin.maxcompute.entity.internal.instance.InstanceStatusModel;
import tech.dingxin.maxcompute.entity.internal.instance.SQL;
import tech.dingxin.maxcompute.utils.CommonUtils;
import tech.dingxin.maxcompute.utils.SqlRunner;
import tech.dingxin.maxcompute.utils.XmlUtils;

import java.net.URI;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static com.aliyun.odps.rest.SimpleXmlUtils.marshal;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@RestController
public class InstanceController {
    private static final Logger LOG = LoggerFactory.getLogger(InstanceController.class);

    Map<String, Map<String, SQLResult>> instanceResultMap;

    public InstanceController() {
        instanceResultMap = new HashMap<>();
    }

    @PostMapping("/projects/{projectName}/instances")
    @ResponseBody
    public ResponseEntity<Object> createInstance(
            @PathVariable("projectName") String projectName,
            @RequestParam("curr_project") String currProject,
            @RequestBody String body) throws SQLException {

        Instance instance = XmlUtils.parseInstance(body);
        SQL sql = instance.getJob().getTasks().getSql();
        String name = sql.getName();
        String query = sql.getQuery().toUpperCase().trim();
        String instanceId = CommonUtils.generateUUID();
        LOG.info("create instance {} to execute query {}", instanceId, query);

        String result = SqlRunner.execute(query);
        LOG.info("instance {} result {}", instanceId, result);

        instanceResultMap.putIfAbsent(instanceId, new HashMap<>());
        instanceResultMap.get(instanceId).put(name, new SQLResult(query, result));

        HttpHeaders headers = new HttpHeaders();
        headers.setLocation(URI.create("/" + instanceId));
        return new ResponseEntity<>(new MessageResponse("Created"), headers, HttpStatus.CREATED);
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

        return new ResponseEntity<>(marshal(new InstanceStatusModel()), headers, HttpStatus.OK);
    }

    @GetMapping(value = "/projects/{projectName}/instances/{instanceId}", params = "taskstatus")
    @ResponseBody
    public String checkInstanceStatus(@PathVariable("projectName") String projectName,
                                      @PathVariable("instanceId") String instanceId,
                                      @RequestParam("curr_project") String currProject) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<Instance><Status>Terminated</Status><Tasks><Task Type=\"SQL\"><Name>AnonymousSQLTask</Name><StartTime>Sat, " +
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
