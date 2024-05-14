package tech.dingxin.maxcompute.utils;

import tech.dingxin.maxcompute.entity.internal.instance.Instance;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class XmlUtils {
    public static Instance parseInstance(String xmlContent) {
        try {
            JAXBContext context = JAXBContext.newInstance(Instance.class);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            return (Instance) unmarshaller.unmarshal(new StringReader(xmlContent));
        } catch (JAXBException e) {
            e.printStackTrace();
            return null;
        }
    }
}
