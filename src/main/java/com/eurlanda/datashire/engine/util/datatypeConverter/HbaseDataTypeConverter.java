package com.eurlanda.datashire.engine.util.datatypeConverter;

import com.eurlanda.datashire.engine.entity.TColumn;

/**
 * Created by zhudebin on 14-7-10.
 */
public class HbaseDataTypeConverter extends TDataTypeConverter {
    @Override
    Object convertData(TColumn tColumn, Object data) {
//        switch (tColumn.getDbBaseDatatype()) {
//            case BOOLEAN:
//                if (tColumn.getJdbcDataType() == TDataType.INT) {
//                    switch (tColumn.getData_type()) {
//                        case BOOLEAN:
//                            return (int) data == 1;
//                        default:
//                            throwException("timestampe类型转换不支持转换为" + tColumn.getData_type());
//                    }
//                } else {
//                    throwException(" timestamp jdbc 只能支持 string");
//                }
//                break;
//
//        }
        return null;
    }
}
