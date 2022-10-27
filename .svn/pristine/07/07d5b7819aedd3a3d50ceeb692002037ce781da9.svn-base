package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.spark.util.ScalaMethodUtil;

import java.math.BigDecimal;

/**
 * Created by zhudebin on 14-7-21.
 */
public class ClassUtil {

    public static Double convert2Double(Object obj) {

        if(obj == null) {
            return null;
        } else {
            if(obj instanceof Integer) {
                return ScalaMethodUtil.toDouble((Integer)obj);
            } else if(obj instanceof Double) {
                return (Double)obj;
            } else if(obj instanceof Float) {
                return ScalaMethodUtil.toDouble((Float)obj);
            } else if(obj instanceof BigDecimal) {
                return ((BigDecimal)obj).doubleValue();
            }else {
                throw new RuntimeException("不能转换的数据类型");
            }

        }

    }

}
