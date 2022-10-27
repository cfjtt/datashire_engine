package com.eurlanda.datashire.engine.java;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.eurlanda.datashire.engine.util.ListUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhudebin on 14-7-24.
 */
public class JavaTest {

    public static void main(String[] args) {

        testReg();

    }

    public static void testReg() {
        String csv = "xxxx,bbb,cc,\"aaa,bbb\"";
        Pattern pattern = Pattern.compile("(\".*\",? | [^,],?)*");
        Matcher matcher = pattern.matcher(csv);
        int count = matcher.groupCount();
        System.out.println(count);
        while(matcher.find()) {
            System.out.println(matcher.group(0));
        }
    }

    @Test
    public void testJson() {
        String str = "[[63790,63791],[2,[49]],[3,[49]],[4,[49]],[5,[49]]]";
        JSONArray array = JSONArray.parseArray(str);
        System.out.println(array);
    }

    @Test
    public void testJson2() {
        String str = "{\"code\":1,\"info\":{\"DebugInfo\":{\"data\":[[63790,63791],[2,[49]],[3,[49]],[4,[49]],[5,[49]]]}}}";
        JSON json = (JSON)JSON.parse(str);

//        JSONArray array = JSONArray.parseArray(str);
//        System.out.println(array);
    }

    @Test
    public void testJson3() {

        List<List<Object>> lists = new ArrayList<>();

        lists.add(ListUtil.<Object>asList(100,"1".getBytes()));
        lists.add(ListUtil.<Object>asList(1,"1".getBytes()));
        lists.add(ListUtil.<Object>asList(2,"1".getBytes()));
        lists.add(ListUtil.<Object>asList(3,"1".getBytes()));

        System.out.println(JSONArray.toJSONString(lists));
        String jsonStr = JSON.toJSONString(lists);
        System.out.println(jsonStr);

        JSONArray array = (JSONArray)JSONArray.parse(jsonStr);
        System.out.println(array);
    }

    @Test
    public void testReg2() {
        String str = "a>100 and b<100";
        String str2 = str.replaceAll("\\s(?i)and", " && ");
        System.out.println(str2);
    }

    @Test
    public void testByte() {
        Byte b = (byte)(Byte.valueOf("1").byteValue() + Byte.valueOf("2").byteValue());
        System.out.println(b);
    }
}
