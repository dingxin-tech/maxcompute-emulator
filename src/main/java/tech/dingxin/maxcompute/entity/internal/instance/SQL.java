package tech.dingxin.maxcompute.entity.internal.instance;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */

import javax.xml.bind.annotation.XmlElement;

public class SQL {
    private String name;
    private Config config;
    private String query;

    @XmlElement(name = "Name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @XmlElement(name = "Config")
    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    @XmlElement(name = "Query")
    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}

