package com.eurlanda.datashire.engine.translation;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhudebin on 14-7-25.
 */
public class TestReg {

    public static void main(String[] args) {
        org.apache.commons.cli.ParseException x;
        test1();
    }

    public static void test1() {
        Pattern pattern = Pattern.compile("([^\\.]*)\\s+>\\s+\\(Select Max\\((.*)\\)\\s+From\\s+([^\\.]*)\\.([^\\.]*)\\)");
        Matcher matcher = pattern.matcher("rowguid > (Select Max(rowguid) From s_table_ld.sf014_zl_stable)");
        int count = matcher.groupCount();
        System.out.println(count);
        while(matcher.find()) {
            System.out.println("列名:" + matcher.group(1));
            System.out.println("目标squid比较列:" + matcher.group(2));
            System.out.println("目标squid:" + matcher.group(3));
            System.out.println("目标squid表名:" + matcher.group(4));
        }
    }

    public static void test() {
        Pattern pattern = Pattern.compile("([^\\.]*){1}\\.([^\\.]*)\\.([^\\.]*)\\s>\\s\\(Select Max\\(([^\\.]*).([^\\.]*)\\)\\sFrom\\s([^\\.]*)\\)");
        Matcher matcher = pattern.matcher("DBSource.sf137_prod.rowguid > (Select Max(sf137_prod.rowguid) From sf137_prod)");
        String g1 = matcher.group(1);
        System.out.println(g1);
    }
}
