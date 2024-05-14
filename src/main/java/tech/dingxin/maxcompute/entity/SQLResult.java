package tech.dingxin.maxcompute.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@AllArgsConstructor
@Data
public class SQLResult {
    private String query;
    private String result;
}
