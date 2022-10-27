package com.eurlanda.common;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by zhudebin on 15-4-16.
 */
public class JarTest {

    @Test
    public void test111() {

        String str = "aaa'aaa'aaa'bbb''cc".replace("'","''");
        System.out.println(str);
    }

    @Test
    public void test3() {
        try {
        } catch (Exception e) {
            e.printStackTrace();
        }


        List<String> list = new LinkedList<>();
        for(int i=0; i<10; i++) {
            list.add(i + "");
        }
//        list.add(10, "hello");
        list.set(10, "hello");
//        list.set(2, "h2");
//        list.add(16, "h15");
//        list.add(-1, "h-1");
//        list.set(15, "h15");
        System.out.println(list);
        System.out.println(list.size());
    }

    public void test1() {
        org.roaringbitmap.RoaringBitmap rb;
    }

    @Test
    public void test2() {

        io.netty.bootstrap.Bootstrap bs;
        org.dom4j.DocumentHelper d;

        String str = "a,b,c,d,e";
        String[] strs = str.split(",");
        for(String s : strs) {
            System.out.println(s);
        }

        System.out.println("---------------");

        strs = str.split(",", 3);
        for(String s : strs) {
            System.out.println(s);
        }

        System.out.println(100.1);

        BigDecimal bd;
        bd = new BigDecimal(new Double(100.1).toString());
        bd = bd.divideAndRemainder(new BigDecimal(10))[1];
        System.out.println(bd);
        System.out.println(100.1 % 3);
        System.out.println(100.1 % 10);
    }

    @Test
    public void test4() {
        int i = (int)(7.9 - 8.1);
        System.out.println(i);
    }

    @Test
    public void test5() {
        float f1 = 1.1f;
        double d1 = (double)f1;
        System.out.println(d1);
        System.out.println(d1-0.1);

        short s1 = new Short("10");
        int i1 = (int)s1;
        System.out.println(i1);
        System.out.println(i1-2);
    }

    @Test
    public void test6() {
        byte b1 = 107;
        System.out.println(b1);
        short s1 = (short)b1;
        System.out.println(s1);

        for(int i=1; i<100; i++) {
            b1 = ++b1;
            System.out.println(b1);
        }
    }
}
