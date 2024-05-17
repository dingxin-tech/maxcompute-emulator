package tech.dingxin.maxcompute.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Data
@EqualsAndHashCode
public class TableId {
    private String tableName;
    private String partitionName;

    public static TableId of(String tableName, String partitionName) {
        TableId tableId = new TableId();
        tableId.tableName = tableName;
        tableId.partitionName = partitionName;
        return tableId;
    }
}
