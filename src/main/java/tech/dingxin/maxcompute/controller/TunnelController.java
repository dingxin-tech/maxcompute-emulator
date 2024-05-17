package tech.dingxin.maxcompute.controller;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import tech.dingxin.maxcompute.entity.SqlLiteColumn;
import tech.dingxin.maxcompute.entity.TableId;
import tech.dingxin.maxcompute.service.TableService;
import tech.dingxin.maxcompute.utils.CommonUtils;
import tech.dingxin.maxcompute.utils.Deserializer;
import tech.dingxin.maxcompute.utils.SqlRunner;
import tech.dingxin.maxcompute.utils.TypeConvertUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@RestController
public class TunnelController {

    private Map<String, TableId> upsertSessionMap;

    @Autowired
    private TableService tableService;

    public TunnelController() {
        upsertSessionMap = new HashMap<>();
    }

    @PostMapping("/projects/{projectName}/tables/{tableId}/upserts")
    @ResponseBody
    public ResponseEntity<String> createOrCommitUpsertSession(
            @PathVariable("tableId") String tableId,
            @RequestParam(value = "partition", required = false, defaultValue = "") String partition,
            @RequestParam(value = "upsertid", required = false, defaultValue = "") String upsertId
    ) {
        JsonObject result = new JsonObject();
        boolean commit = false;
        // sessionId
        if (upsertId.isEmpty()) {
            upsertId = CommonUtils.generateUUID();
            upsertSessionMap.put(upsertId, TableId.of(tableId, partition));
        } else {
            upsertSessionMap.remove(upsertId);
            commit = true;
        }

        result.add("id", new JsonPrimitive(upsertId));
        // tunnelTableSchema
        JsonObject schema = new JsonObject();
        JsonArray columns = new JsonArray();
        JsonArray hashKeys = new JsonArray();

        List<SqlLiteColumn> sqlLiteSchema = tableService.getSchema(tableId);
        for (int cid = 0; cid < sqlLiteSchema.size(); cid++) {
            SqlLiteColumn column = sqlLiteSchema.get(cid);
            JsonObject columnJson = new JsonObject();
            columnJson.add("name", new JsonPrimitive(column.getName()));
            columnJson.add("type",
                    new JsonPrimitive(TypeConvertUtils.convertToMaxComputeType(column.getType()).getTypeName()));
            columnJson.add("nullable", new JsonPrimitive(column.isNotNull()));
            columnJson.add("column_id", new JsonPrimitive(cid));
            columns.add(columnJson);
            if (column.isPrimaryKey()) {
                hashKeys.add(new JsonPrimitive(column.getName()));
            }
        }
        schema.add("columns", columns);
        //TODO: schema.add("partitionKeys", columns);
        result.add("schema", schema);

        // hash_key
        result.add("hash_key", hashKeys);

        // hasher
        result.add("hasher", new JsonPrimitive("default"));

        // slots
        JsonArray slots = new JsonArray();
        JsonObject slot = new JsonObject();
        slot.add("slot_id", new JsonPrimitive(0));
        JsonArray buckets = new JsonArray();
        buckets.add(0);
        slot.add("buckets", buckets);
        slot.add("worker_addr", new JsonPrimitive("127.0.0.1:8080"));
        slots.add(slot);

        result.add("slots", slots);
        if (commit) {
            result.add("status", new JsonPrimitive("committed"));
        } else {
            result.add("status", new JsonPrimitive("normal"));
        }

        HttpHeaders headers = new HttpHeaders();
        headers.set("x-odps-request-id", upsertId);
        return new ResponseEntity<>(result.toString(), headers, HttpStatus.OK);
    }

    @GetMapping("/projects/{projectName}/tables/{tableId}/upserts")
    @ResponseBody
    public String reloadUpsertSession() {
        return "";
    }

    @PutMapping("/projects/{projectName}/tables/{tableId}/upserts")
    @ResponseBody
    public ResponseEntity flushData(
            @PathVariable("tableId") String tableId,
            @RequestParam(value = "partition", required = false, defaultValue = "") String partition,
            @RequestParam(value = "upsertid", required = true, defaultValue = "") String sessionId,
            @RequestHeader(value = "Content-Encoding", required = false, defaultValue = "") String compression,
            @RequestBody byte[] requestBody
    ) throws IOException {
        List<Object[]> records = Deserializer.deserializeData(new ByteArrayInputStream(requestBody),
                tableService.getSchema(tableId).stream()
                        .map(c -> TypeConvertUtils.convertToMaxComputeType(c.getType())).collect(Collectors.toList()));
        SqlRunner.upsertData(tableId, records, tableService.getSchema(tableId));
        HttpHeaders headers = new HttpHeaders();
        headers.set("x-odps-request-id", sessionId);
        return new ResponseEntity<>("OK", headers, HttpStatus.OK);
    }

    @GetMapping("/projects/{projectName}/tunnel")
    @ResponseBody
    public String getTunnelEndpoint() {
        return "127.0.0.1:8080";
    }
}