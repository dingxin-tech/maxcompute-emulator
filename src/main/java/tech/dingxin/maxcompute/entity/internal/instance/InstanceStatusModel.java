package tech.dingxin.maxcompute.entity.internal.instance;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Root(name = "Instance", strict = false)
public class InstanceStatusModel {
    @Element(name = "Status", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String status;

    public InstanceStatusModel() {
        this.status = "Terminated";
    }

    public InstanceStatusModel(String status) {
        this.status = status;
    }
}