package com.eurlanda.datashire.engine.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Created by zhudebin on 14-6-5.
 */
public class MathUtil {

    final static int[] sizeTable = {1, 10, 100, 1000, 10000, 100000, 1000000,
            10000000, 100000000, 1000000000};

    //默认除法运算精度
    private static final int DEF_DIV_SCALE = 10;

    public static int lengthOfInt(int i) {
        for (int idx = sizeTable.length; idx > 0; idx--) {
            if (i / sizeTable[idx - 1] > 0) {
                return idx;
            }
        }
        return 1;
    }

    /**
     * 获取小数位为 m,非小数为d的最大BigDecimal
     *
     * @param d 非小数位数
     * @param m 小数位数
     * @return
     */
    public static BigDecimal maxDecimal(int d, int m) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (d - m); i++) {
            sb.append("9");
        }
        if(d-m == 0) {
            sb.append("0");
        }
        sb.append(".");
        for (int i = 0; i < m; i++) {
            sb.append("9");
        }
        if(m == 0) {
            sb.append("0");
        }
        return new BigDecimal(sb.toString());
    }

    /**
     * 精度为 precision 的最大double 值
     *
     * @param precision
     * @return
     */
    public static double maxDouble(int precision) {
        /**
         if(precision == 0) {
         return 0;
         }
         StringBuilder sb = new StringBuilder();
         for(int i=0; i< precision; i++) {
         sb.append("9");
         }
         return Double.parseDouble(sb.toString());
         */

        double max = 1;
        for (int i = 0; i < precision; i++) {
            max *= 10;
        }
        return max - 1;

    }

    /**
     * 格式化bigDecimal
     *
     * @param bd        待格式化的数据
     * @param scale     小数点位数
     * @return
     */
    public static BigDecimal formatBigDecimal(BigDecimal bd, int scale) {
       // bd = bd.setScale(scale, RoundingMode.HALF_EVEN).stripTrailingZeros();
       // return bd;
        String bdstr = bd.setScale(scale, RoundingMode.HALF_EVEN).stripTrailingZeros().toPlainString();//避免10，20这么小的数用科学计数法
        return new BigDecimal(bdstr);
    }

    /**
     * 提供精确的加法运算。
     *
     * @param v1 被加数
     * @param v2 加数
     * @return 两个参数的和
     */
    public static double add(double v1, double v2) {
        BigDecimal b1 = new BigDecimal(Double.toString(v1));
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return b1.add(b2).doubleValue();
    }

    /**
     * 提供精确的减法运算。
     *
     * @param v1 被减数
     * @param v2 减数
     * @return 两个参数的差
     */
    public static double sub(double v1, double v2) {
        BigDecimal b1 = new BigDecimal(Double.toString(v1));
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return b1.subtract(b2).doubleValue();
    }

    /**
     * 提供精确的乘法运算。
     *
     * @param v1 被乘数
     * @param v2 乘数
     * @return 两个参数的积
     */
    public static double mul(double v1, double v2) {
        BigDecimal b1 = new BigDecimal(Double.toString(v1));
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return b1.multiply(b2).doubleValue();
    }

    /**
     * 提供（相对）精确的除法运算，当发生除不尽的情况时，精确到
     * 小数点以后10位，以后的数字四舍五入。
     *
     * @param v1 被除数
     * @param v2 除数
     * @return 两个参数的商
     */
    public static double div(double v1, double v2) {
        return div(v1, v2, DEF_DIV_SCALE);
    }

    /**
     * 提供（相对）精确的除法运算。当发生除不尽的情况时，由scale参数指
     * 定精度，以后的数字四舍五入。
     *
     * @param v1    被除数
     * @param v2    除数
     * @param scale 表示表示需要精确到小数点以后几位。
     * @return 两个参数的商
     */
    public static double div(double v1, double v2, int scale) {
        if (scale < 0) {
            throw new IllegalArgumentException(
                    "The scale must be a positive integer or zero");
        }
        BigDecimal b1 = new BigDecimal(Double.toString(v1));
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return b1.divide(b2, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    /**
     * 提供精确的小数位四舍五入处理。
     *
     * @param v     需要四舍五入的数字
     * @param scale 小数点后保留几位
     * @return 四舍五入后的结果
     */
    public static double round(double v, int scale) {
        if (scale < 0) {
            throw new IllegalArgumentException("The scale must be a positive integer or zero");
        }
        BigDecimal b = new BigDecimal(Double.toString(v));
        BigDecimal one = new BigDecimal("1");
        return b.divide(one, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }


    public static void main(String[] args) {
//        System.out.println(lengthOfInt(100000000));
//        System.out.println(maxDecimal(5, 0));
        /**
        BigDecimal bd = new BigDecimal("999999.99999999");
        Double d1 = formatBigDecimal(bd, 8, 2).doubleValue();
        Double d = bd.doubleValue();

        System.out.println(d);
        System.out.println(d1);
         */
        Object i = 10;
        double d = (Double)i - 20.1;
        System.out.println(d);
    }

}
