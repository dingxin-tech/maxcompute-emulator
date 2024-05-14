package tech.dingxin.maxcompute.utils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CommonUtils {
    public static String generateUUID() {
        return java.util.UUID.randomUUID().toString().replace("-", "");
    }
}
