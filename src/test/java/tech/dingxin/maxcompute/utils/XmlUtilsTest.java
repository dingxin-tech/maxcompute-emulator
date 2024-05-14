package tech.dingxin.maxcompute.utils;

import org.junit.jupiter.api.Test;
import tech.dingxin.maxcompute.entity.Instance;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class XmlUtilsTest {
    @Test
    public void testInstance() {
        String intanceXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<Instance>\n" +
                "   <Job>\n" +
                "      <Priority>9</Priority>\n" +
                "      <Tasks>\n" +
                "         <SQL>\n" +
                "            <Name>AnonymousSQLTask</Name>\n" +
                "            <Config>\n" +
                "               <Property>\n" +
                "                  <Name>type</Name>\n" +
                "                  <Value>sql</Value>\n" +
                "               </Property>\n" +
                "               <Property>\n" +
                "                  <Name>uuid</Name>\n" +
                "                  <Value>75d0e8af-f6f8-4c55-93ce-3e2bbf042d02</Value>\n" +
                "               </Property>\n" +
                "               <Property>\n" +
                "                  <Name>settings</Name>\n" +
                "                  <Value>{&quot;odps.idata.userenv&quot;:&quot;JavaSDK Revision:,Version:0.48.2-public,JavaVersion:1.8.0_291,IP:30.221.116.113,MAC:A4-CF-99-97-35-F6&quot;}</Value>\n" +
                "               </Property>\n" +
                "            </Config>\n" +
                "            <Query>select * from test_table</Query>\n" +
                "         </SQL>\n" +
                "      </Tasks>\n" +
                "   </Job>\n" +
                "</Instance>";
        Instance instance = XmlUtils.parseInstance(intanceXml);

        System.out.println(instance);
    }
}
