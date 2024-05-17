package tech.dingxin.maxcompute.controller;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import tech.dingxin.maxcompute.entity.internal.project.ProjectModel;

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
