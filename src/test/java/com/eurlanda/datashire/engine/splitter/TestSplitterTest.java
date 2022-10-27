package com.eurlanda.datashire.engine.splitter;

import com.eurlanda.datashire.engine.spark.db.TextSplitter;
import com.eurlanda.datashire.enumeration.DataBaseType;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * Created by zhudebin on 15/12/3.
 */
public class TestSplitterTest {

    @Test
    public void test1() {
        TextSplitter ts = new TextSplitter(1, null, DataBaseType.HBASE_PHOENIX);

        String str1 = "吴江";
        String str2 = "北京";

        int c1 = str1.compareTo(str2);

        System.out.println(c1);

        BigDecimal bd1 = ts.stringToBigDecimal(str1);
        BigDecimal bd2 = ts.stringToBigDecimal(str2);

        System.out.println(bd1);
        System.out.println(bd2);
    }

    @Test
    public void test2() {
        String[] strs = new String[]{"上海","北京"};
        Arrays.sort(strs);
        for(String str : strs) {
            System.out.println(str);
        }
    }

    @Test
    public void test3() {
        String[] strs = new String[]{"上海","北京","南京","北a","cabd","北"};
        for(String str : strs) {
            byte[] bytes = str.getBytes();
            System.out.println(str + " : ");
            for(byte b : bytes) {
                System.out.print(b + ",");
            }
            System.out.println();
        }

    }
}
