package com.eurlanda.datashire.engine.util;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * apache 两种日志格式：
 * 1.Common Log Format
 * 2.Apache Combined
 * Created by Juntao.Zhang on 2014/5/23.
 */
public class ApacheLogFormat {
    private static class Info {
        String orgString;
        String newString;
        String result;
    }

    private static Properties p = new Properties();
    public final static String DEFAULT_COMMON_LOG_FORMAT = "%h %l %u %t \"%r\" %>s %b";
    public final static String APACHE_COMBINED_FORMAT = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"";
    private final static Map<String, String> CODE_NAME_MAPPING = new HashMap<>();

    static {
        try {
            p.load(ApacheLogFormat.class.getResourceAsStream("/conf/apacheLogFormatRegExp.properties"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        CODE_NAME_MAPPING.put("%h", "ip");
        CODE_NAME_MAPPING.put("%l", "user_identifier");
        CODE_NAME_MAPPING.put("%u", "userid");
        CODE_NAME_MAPPING.put("%t", "request_datetime");
        CODE_NAME_MAPPING.put("\"%r\"", "request");
        CODE_NAME_MAPPING.put("%>s", "http_status_code");
        CODE_NAME_MAPPING.put("%b", "resource_size");
        CODE_NAME_MAPPING.put("%B", "resource_size");
        CODE_NAME_MAPPING.put("\"%{Referer}i\"", "referer");
        CODE_NAME_MAPPING.put("\"%{User-agent}i\"", "user_agent");
    }

    private static String getProperty(String key) {
        return p.getProperty(key);
    }


    public static Map<String, String> logInfoHandler(String str, String[] apacheLogFormatNames) {
        Map<String, String> result = new HashMap<>();
        if (org.apache.commons.lang.StringUtils.isBlank(str)) return result;
        Info info;
        for (String key : apacheLogFormatNames) {
            info = getInfo(str, getProperty(key));
            result.put(CODE_NAME_MAPPING.get(key), info.result);
            str = info.newString;
        }
        return result;
    }

    private static Info getInfo(String str, String reg) {
        Info info = new Info();
        str = org.apache.commons.lang.StringUtils.trim(str);
        info.orgString = str;
        if(StringUtils.isNotEmpty(str)) {
            Pattern pattern = Pattern.compile(reg);
            Matcher matcher = pattern.matcher(str);
            if (matcher.find()) {
                info.result = org.apache.commons.lang.StringUtils.trim(matcher.group());
                info.newString = str.substring(matcher.end());
            }
        }
        return info;
    }

}
