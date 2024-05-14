package tech.dingxin.maxcompute.entity.internal.table;

import com.aliyun.odps.Table;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.Text;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import lombok.Builder;
import lombok.Data;

import java.util.Date;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Root(name = "Table", strict = false)
@Data
public class TableModel {

    @Root(name = "Schema", strict = false)
    static class Schema {
        @Text(required = false)
        String content;
    }

    @Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String name;

    @Element(name = "Schema", required = false)
    private Schema schema;

    @Element(name = "CreationTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date createdTime;

    @Element(name = "LastModifiedTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastModifiedTime;

    @Element(name = "LastAccessTime", required = false)
    @Convert(SimpleXmlUtils.DateConverter.class)
    Date lastAccessTime;

    @Element(name = "Type", required = false)
    @Convert(Table.TableTypeConverter.class)
    Table.TableType type;

    public TableModel(String name, String schema) {
        this.name = name;
        this.schema = new Schema();
        this.schema.content = schema;
        this.createdTime = new Date();
        this.lastModifiedTime = new Date();
        this.lastAccessTime = new Date();
        this.type = Table.TableType.MANAGED_TABLE;
    }
}


