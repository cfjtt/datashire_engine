package com.eurlanda.common;

import org.junit.Test;

/**
 * Created by zhudebin on 15-8-31.
 */
public class RegTest {

    @Test
    public void reg1() {
        String filterString = "doc_imdb_doc.Title = ' 11222.aaaaaa' and aa.bb = '1111.bbb'";

        String fs = filterString.replaceAll("(?<=[^><=!])=(?=[^><=])", " == ")
                .replaceAll("\\s(?i)and", " && ")
                .replaceAll("\\s(?i)or", " || ")
//                .replaceAll("#|(\\w+)\\.(?!\\d+)", "$1\\$")
                .replaceAll("\\s#|((\\s\\w+)|(^\\w+))\\.(?!\\d+)", "$1\\$")
                .replace("'", "\"")
                .replace("<>", "!=");

        System.out.println(fs);

        String filterStr = "'11222.aaaaaa'";
        String reg = "\\s#|((\\s\\w+)|(^\\w+))\\.(?!\\d+)";
        String a = filterString.replaceAll(reg, "$1\\$");
        String b = filterStr.replaceAll(reg, "$1\\$");
        System.out.println(a);
        System.out.println(b);
    }

    @Test
    public void test2() {
        String s = "1-2-3-4";
        String[] strs = s.split("-");
        for(String ss : strs) {
            System.out.println(ss);
        }
    }

    @Test
    public void test3() {
        String s = "abab1223abc123abab";
        for(String ss : s.split("ab", -1)) {
            System.out.println("|" + ss + "|");
        }

        Integer gb = Integer.MAX_VALUE / (1024*1024);
        System.out.println(gb);
        gb = 2147483647 / (1024*1024);
        System.out.println(gb);
    }

}
