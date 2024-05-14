package tech.dingxin.maxcompute.entity.internal.instance;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */

import javax.xml.bind.annotation.XmlElement;

public class Tasks {
    private SQL sql;

    @XmlElement(name = "SQL")
    public SQL getSql() {
        return sql;
    }

    public void setSql(SQL sql) {
        this.sql = sql;
    }
}
