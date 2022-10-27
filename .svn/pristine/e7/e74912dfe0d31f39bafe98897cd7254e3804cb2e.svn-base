package com.eurlanda.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.eurlanda.datashire.engine.entity.SFLog;
import com.eurlanda.datashire.engine.util.JsonUtil;
import org.apache.commons.io.input.BOMInputStream;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 2016/11/30.
 */
public class JsonTest {

    @Test
    public void test1() {

        String str = "[[1486731,1486732,1486733,1486734],[\"#\\u0000\",\"#null\",\"#null\",\"#不能将类型转换为INT\"],[\"#\\u00001\\u0000\",\"#\\u0000/f�[�v\",\"#\\u0000s\\u0000h\\u0000j\\u0000d\\u0000\\r\\u0000\",\"#不能将类型转换为INT\"],[\"#\\u00002\\u0000\",\"#\\u0000penc\",\"#\\u0000d\\u0000d\\u0000j\\u0000k\\u0000\\r\\u0000\",\"#不能将类型转换为INT\"],[\"#\\u00003\\u0000\",\"#\\u0000�Sw�\",\"#\\u0000s\\u0000j\\u0000k\\u0000d\\u0000\",\"#不能将类型转换为INT\"]]";

        JSONArray array = (JSONArray)JSONArray.parse(str);
        Iterator iter = array.iterator();
        while(iter.hasNext()) {
            JSONArray arr = (JSONArray)iter.next();
            Iterator it = arr.iterator();
            while(it.hasNext()) {
                Object json = it.next();
                System.out.println(json);
            }
        }
    }

    @Test
    public void test2() {
        Map<String, Object> map = new HashMap<>();
        map.put("111", "222");
        map.put("\"222\"", null);

        String str = JSONObject.toJSONString(map);
        System.out.println(str);

        str = JsonUtil.toJSONString(map);
        System.out.println(str);
    }

    @Test
    public void test3() {
        List<String> list = new ArrayList<>();
        list.add("\"1\"");
        list.add("\"2\"");
        list.add("\"3\"");
        list.add("1");
        list.add(JsonUtil.toJSONString("\"5\""));
        list.add(JsonUtil.toJSONString("\"\5\""));

        String json = JsonUtil.toJSONString(list);
        System.out.println(json);
        JSONArray array = (JSONArray)JSONArray.parse(json);
        Iterator iter = array.iterator();
        while(iter.hasNext()) {
            Object obj = iter.next();
            System.out.println(JSONObject.parse(obj.toString()).toString());
        }

        URLEncoder.encode("");
    }

    @Test
    public void test5() throws UnsupportedEncodingException {
        String str = "%5B%5B1492514%2C1492515%5D%2C%5B%22using%22%2C%221%22%5D%2C%5B%22hello%22%2C%222%22%5D%2C%5B%22their%22%2C%221%22%5D%2C%5B%22I%22%2C%221%22%5D%2C%5B%22of%22%2C%222%22%5D%2C%5B%22hadoop%22%2C%224%22%5D%2C%5B%22build%22%2C%221%22%5D%2C%5B%22%EF%BB%BFI%22%2C%221%22%5D%2C%5B%22is%22%2C%222%22%5D%2C%5B%22infrastructure%22%2C%221%22%5D%2C%5B%22love%22%2C%222%22%5D%2C%5B%22companies%22%2C%221%22%5D%2C%5B%22data%22%2C%222%22%5D%2C%5B%22ecosystem%22%2C%221%22%5D%2C%5B%22the%22%2C%223%22%5D%2C%5B%22initiatives%22%2C%221%22%5D%2C%5B%22source%22%2C%221%22%5D%2C%5B%22an%22%2C%222%22%5D%2C%5B%22to%22%2C%221%22%5D%2C%5B%22project%22%2C%221%22%5D%2C%5B%22big%22%2C%222%22%5D%2C%5B%22are%22%2C%221%22%5D%2C%5B%22world%22%2C%223%22%5D%2C%5B%22open%22%2C%221%22%5D%2C%5B%22many%22%2C%221%22%5D%5D";

        String s2 =  URLDecoder.decode(str, "UTF-8");
        System.out.println(s2);
    }

    @Test
    public void test6() throws Exception {
        String fileName = "/Users/zhudebin/soft/ds/words.txt";

        FileInputStream fis = new FileInputStream(fileName);

        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String line = null;

        while((line=br.readLine()) != null) {
            System.out.println(line);
        }

        BOMInputStream bis = new BOMInputStream(fis);
//        boolean hasBOM = bis.hasBOM();

//        System.out.println(hasBOM);
//        System.out.println(bis.getBOM());
//        System.out.println(bis.getBOMCharsetName());
//        byte[] bs = bis.getBOM().getBytes();
//        System.out.println(bs);


        BufferedReader br2 = new BufferedReader(new InputStreamReader(bis));
        String line2 = null;

        while((line2=br2.readLine()) != null) {
            System.out.println(line2);
        }

    }

    @Test
    public void testLog() {
        SFLog log = new SFLog();
        System.out.println(log.getCreateTime());
        String str = null;
        str = JSON.toJSONString(log);
        System.out.println(str);
        str = JsonUtil.toJSONString(log);
        System.out.println(str);
    }
}
