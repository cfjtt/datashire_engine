package com.eurlanda.datashire.engine.util;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * extended log format 与项目自定义对应关系
 * Created by Juntao.Zhang on 2014/6/17.
 */
public class ExtendedLogFormat {
    private static Properties p = new Properties();
    private static Map<String,String > mapping = new HashMap<>();

    static {
        try {
            p.load(ExtendedLogFormat.class.getResourceAsStream("/conf/extendedLogFormatMapping.properties"));
            Enumeration<?> enumeration =p.propertyNames();
            while (enumeration.hasMoreElements()){
                String key = (String) enumeration.nextElement();
                mapping.put(getValue(key),key);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getValue(String key) {
        return p.getProperty(key);
    }

    public static String getKey(String val){
        return mapping.get(val);
    }

    public static void main(String[] args) {
        System.out.println(getKey("cs(Cookie)"));
        System.out.println(getKey("cookie"));
    }
}
