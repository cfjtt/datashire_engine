package com.eurlanda.datashire.engine.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.eurlanda.datashire.engine.entity.SFLog;
import org.junit.Test;

import java.sql.Date;

/**
 * Created by zhudebin on 14-7-16.
 */
public class JsonTest {

    public static void main(String[] args) {
        SFLog sfLog = new SFLog();
        sfLog.setId(1l);
        sfLog.setTSquidType(2);

        System.out.println(JSON.toJSONString(sfLog));

    }

    @Test
    public void test1() {
        String str = "[\"\\\"1\\\"\",\"\\\"9.2\\\"\",\"\\\"The Shawshank Redemption (1994)\\\"\",\"\\\"\\\\\\\"560\\\"\",\"\\\"2016-05-20 14:11:53.409000000\\\"\"]";

        JSONArray obj = (JSONArray)JSONArray.parse(str);
        System.out.println(obj.get(0));
        System.out.println(obj.get(1));
        System.out.println(obj.get(2));
        System.out.println(obj.get(3));
        System.out.println(obj.get(4));

        System.out.println("---------------");

        String jsonStr = JSONObject.toJSONString("\"229");
        System.out.println(jsonStr);

        Object o = JSONObject.parse(jsonStr);
        System.out.println(o);
        o = JSONObject.parse(obj.get(3).toString());
        System.out.println(o);
    }


}
