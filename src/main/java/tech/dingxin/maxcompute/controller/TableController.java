package tech.dingxin.maxcompute.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import tech.dingxin.maxcompute.entity.internal.table.ListTablesResponse;
import tech.dingxin.maxcompute.entity.internal.table.TableModel;
import tech.dingxin.maxcompute.service.TableService;

import static com.aliyun.odps.rest.SimpleXmlUtils.marshal;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@RestController
public class TableController {
    @Autowired
    private TableService tableService;

    @GetMapping("/projects/{projectName}/tables/{tableId}")
    @ResponseBody
    public String getInstance(@PathVariable("projectName") String projectName,
                              @PathVariable("tableId") String tableId) throws Exception {
        return marshal(new TableModel(tableId, tableService.reloadTable(tableId).toString()));
    }

    @GetMapping("/projects/{projectName}/schemas/{schemaName}/tables/{tableId}")
    @ResponseBody
    public String getInstance(@PathVariable("projectName") String projectName,
                              @PathVariable("schemaName") String schemaName,
                              @PathVariable("tableId") String tableId) throws Exception {
        return marshal(new TableModel(tableId, tableService.reloadTable(tableId)));
    }

    @GetMapping("/projects/{projectName}/tables")
    @ResponseBody
    public String listTable(@PathVariable("projectName") String projectName,
                            @RequestParam("expectmarker") boolean expectmarker) throws Exception {
        return marshal(new ListTablesResponse(tableService.listTables()));
    }
}
