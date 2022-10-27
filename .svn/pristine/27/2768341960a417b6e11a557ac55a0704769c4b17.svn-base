package com.eurlanda.datashire.engine.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 15-4-1.
 */
public class JsonUtil {

    private static SerializeConfig conf;

    static {
        conf = SerializeConfig.getGlobalInstance();
        conf.put(byte[].class, CustomByteArraySerializer.instance);
        conf.put(Date.class, CustomDateSerializer.instance);
        conf.put(Timestamp.class, CustomTimestampSerializer.instance);
        conf.put(Time.class, CustomTimeSerializer.instance);
//        conf.put(List.class, CustomObjectStrSerializer.instance);
//        conf.put(HashMap.class, CustomObjectStrSerializer.instance);
    }

    public static String toJSONString(Object obj) {
        return JSONObject.toJSONString(obj, conf, SerializerFeature.WriteMapNullValue);
    }

    static class CustomByteArraySerializer implements ObjectSerializer {

        public static CustomByteArraySerializer instance = new CustomByteArraySerializer();

        @Override
        public void write(JSONSerializer serializer, Object object, Object o1, Type type, int i)
                throws IOException {
            SerializeWriter out = serializer.getWriter();

            if (object == null) {
                out.writeNull();
                return;
            }

            byte[] array = (byte[]) object;
//            out.write("\"VARBINARY(" + array.length + " bytes)\"");
            out.write("VARBINARY(" + array.length + " bytes)");
        }
    }

    static class CustomDateSerializer implements ObjectSerializer {

        public static CustomDateSerializer instance = new CustomDateSerializer();

        public final void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int i) throws IOException {
            SerializeWriter out = serializer.getWriter();

            if (object == null) {
                out.writeNull();
                return;
            }
            Date date = (Date) object;
//            out.write("\"" + sdf.format(date) + "\"");
            out.write(DateUtil.format("yyyy-MM-dd HH:mm:ss.SSS", date));
        }
    }

    static class CustomTimeSerializer implements ObjectSerializer {

        public static CustomTimeSerializer instance = new CustomTimeSerializer();

        public final void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int i) throws IOException {
            SerializeWriter out = serializer.getWriter();

            if (object == null) {
                out.writeNull();
                return;
            }
            Time date = (Time) object;
//            out.write("\"" + sdf.format(date) + "\"");
            out.write(DateUtil.format("HH:mm:ss", date));
        }
    }

    static class CustomTimestampSerializer implements ObjectSerializer {

        public static CustomTimestampSerializer instance = new CustomTimestampSerializer();

        public final void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int i) throws IOException {
            SerializeWriter out = serializer.getWriter();

            if (object == null) {
                out.writeNull();
                return;
            }
            Timestamp date = (Timestamp) object;
//            out.write("\"" + sdf.format(date) + String.format("%1$09d", date.getNanos()) + "\"");
            out.write(DateUtil.format("yyyy-MM-dd HH:mm:ss.", date) + String.format("%1$09d", date.getNanos()));
//            out.write("\"" + date.toString() + "\"");
        }
    }

    public static void main(String[] args) {
        Timestamp ts = new Timestamp(new java.util.Date().getTime());
        ts.setNanos(8888);
        System.out.println(ts.getNanos());
        System.out.println(DateUtil.format("yyyy-MM-dd HH:mm:ss.", ts) + ts.getNanos());
        ts.setNanos(ts.getNanos() + 1);
        System.out.println(ts.getNanos());
        System.out.println(DateUtil.format("yyyy-MM-dd HH:mm:ss.", ts) + String.format("%1$09d", ts.getNanos()));

        System.out.println(String.format("%1$09d", 3123));
        System.out.println(String.format("%1$9d", -31));
        System.out.println(String.format("%1$-9d", -31));
        System.out.println(String.format("%1$(9d", -31));
        System.out.println(String.format("%1$#9x", 5689));

        Map<String, Object> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", "b");
        map.put("c", null);
        System.out.println(map);
        List<Object> list = new ArrayList<Object>();
        list.add("a");
        list.add("b");
        list.add(map);
       // System.out.println(list);

       // System.out.println(toJSONString(list));
        System.out.println(toJSONString(map));

        Date date = new Date(new java.util.Date().getTime());
       // System.out.println(toJSONString(date));

    }
}
