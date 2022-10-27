package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.entity.DataCell;

import java.math.BigDecimal;

/**
 * Created by zhudebin on 15-5-24.
 */
public class DataCellUtil {

    public static Object getData(DataCell dc) {
        if(dc == null) {
            return null;
        } else {
            return dc.getData();
        }
    }

    /**
     * 将数据取出,格式必须为Mongodb能够序列化的
     * @param dc
     * @return
     */
    public static Object getMongoData(DataCell dc) {
        if(dc == null) {
            return null;
        } else {
            Object data = dc.getData();
            if(data instanceof java.sql.Date) {
                data = DateUtil.sql2util((java.sql.Date)data);
            }
            if(data instanceof BigDecimal){
                BigDecimal bigDecimal = (BigDecimal) data;
                data = bigDecimal.doubleValue();
            }
            return data;
        }
    }
}
