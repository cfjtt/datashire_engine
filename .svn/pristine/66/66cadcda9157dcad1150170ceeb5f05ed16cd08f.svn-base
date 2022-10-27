package com.eurlanda.datashire.engine.util.datatypeConverter;

import com.eurlanda.datashire.engine.entity.TColumn;

import java.io.Serializable;

/**
 * Created by zhudebin on 14-7-10.
 */
public abstract class TDataTypeConverter implements Serializable {

    public Object convert(TColumn tColumn, Object data) {
        if (data == null) {
            return data;
        } else {
            return convertData(tColumn, data);
        }
    }

    /**
     * 将需要Jdbc读取类型转换为目标类型
     *
     * @param tColumn
     * @param data    data的类型与tColumn的 jdbcDataType是一致的
     * @return
     */
    abstract Object convertData(TColumn tColumn, Object data);

    protected void throwException(String expMes) {
        throw new RuntimeException("sqlserver 不支持该数据类型转换," + expMes);
    }
}
