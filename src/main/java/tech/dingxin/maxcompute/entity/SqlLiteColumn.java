package tech.dingxin.maxcompute.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Data
@AllArgsConstructor
public class SqlLiteColumn {
    String name;
    String type;
    boolean notNull;
    String defaultValue;
    boolean primaryKey;
}
