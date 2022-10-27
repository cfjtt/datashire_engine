package com.eurlanda.common;

import org.junit.Test;

import java.math.BigDecimal;

/**
 * Created by zhudebin on 16/6/17.
 */
public class MathTest {

    @Test
    public void testCeil() {
        System.out.println(Math.ceil(1.1));
    }

    @Test
    public void testBigdecimal() {
        BigDecimal bd = new BigDecimal("11111.111");
//        System.out.println(bd.precision());
//        System.out.println(bd.scale());
        BigDecimal bd2 = bd.multiply(new BigDecimal("1"));
//        bd.setScale(10, BigDecimal.ROUND_HALF_UP);
        System.out.println(bd);
        System.out.println(bd2);
        System.out.println(bd2.setScale(10, BigDecimal.ROUND_HALF_UP));

        System.out.println(bd2);
        System.out.println(bd.scale());
        System.out.println(bd.doubleValue());
        System.out.println(bd.floatValue());
        System.out.println(new BigDecimal(bd.doubleValue()));


    }
}
