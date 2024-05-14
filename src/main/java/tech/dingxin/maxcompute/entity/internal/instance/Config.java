package tech.dingxin.maxcompute.entity.internal.instance;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
import java.util.List;
import javax.xml.bind.annotation.XmlElement;

public class Config {
    private List<Property> properties;

    @XmlElement(name = "Property")
    public List<Property> getProperties() {
        return properties;
    }

    public void setProperties(List<Property> properties) {
        this.properties = properties;
    }
}
