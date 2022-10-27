package com.eurlanda.datashire.engine.util.datatypeConverter;

import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataType;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * Created by zhudebin on 14-7-10.
 */
public class MysqlDataTypeConverter extends TDataTypeConverter {
    @Override
    Object convertData(TColumn tColumn, Object data) {
        switch (tColumn.getDbBaseDatatype()) {
            case YEAR:
                if(tColumn.getJdbcDataType() == TDataType.DATE) {
                    if(tColumn.getData_type() == TDataType.TIMESTAMP) {
                        return new Timestamp(((Date)data).getTime());
                    }
                }
                throw new RuntimeException("无法转换该类型,column " + tColumn);
        }
        return null;
    }
}
