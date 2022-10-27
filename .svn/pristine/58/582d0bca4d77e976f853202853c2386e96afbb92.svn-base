package com.eurlanda.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringEscapeUtils;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

/**
 * Created by zhudebin on 16/5/12.
 */
public class ToStringTest {

    @Test
    public void test1() {
        byte[] bytes = "helloworld".getBytes();

        for(Byte b : bytes) {
            System.out.println(b.toString());
        }
        System.out.println(bytes.toString());
    }

    @Test
    public void test2() {
        String s1 = "\n";
        System.out.println(s1);
        try {
            byte[] bs = s1.getBytes("utf-8");
            System.out.println(bs);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        String s2 = "\\n";
        System.out.println(s2);
        try {
            byte[] bs2 = s2.getBytes("utf-8");
            System.out.println(bs2);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test3() {
        String s1 = "\n";
        String s2 = "\\n";

        String s3 = StringEscapeUtils.escapeJava(s1);
        System.out.println(s3);
        String s4 = StringEscapeUtils.escapeJava(s2);
        System.out.println(s4);

        String s2_1 = StringEscapeUtils.unescapeJava(s2);
        System.out.println(s2_1);

        JSONObject json = new JSONObject();
        json.put("h1", s1);
        String jsonStr = json.toJSONString();
        System.out.println(jsonStr);

        json = JSONObject.parseObject(jsonStr);
        System.out.println(json);

//        java.lang.TypeNotPresentException
    }

    @Test
    public void test4() {
        String str = "12345678";
        System.out.println(str.substring(0, 10));
    }

    @Test
    public void test5() {
        String squidName = "xx_p1";
        if(squidName.length() > 2) {
            int idx = squidName.lastIndexOf("_p");
            if(idx > 0) {
                System.out.println(idx);
                String rePartitionNumStr = squidName.substring(idx + 2);
                try {
                    int rePartitionNum = Integer.parseInt(rePartitionNumStr);
                    System.out.println(rePartitionNum);
                } catch (Exception e) {
                    System.out.println("转换分区数异常,rePartitionNumStr:" + rePartitionNumStr);
                }
            }
        }
    }
}
