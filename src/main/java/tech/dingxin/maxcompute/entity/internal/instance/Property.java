package tech.dingxin.maxcompute.entity.internal.instance;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */

import javax.xml.bind.annotation.XmlElement;

public class Property {
    private String name;
    private String value;

    @XmlElement(name = "Name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @XmlElement(name = "Value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

