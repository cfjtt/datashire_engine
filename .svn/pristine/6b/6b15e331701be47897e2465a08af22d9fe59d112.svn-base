package com.eurlanda.datashire.engine.util.datatypeConverter;

import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.util.DateUtil;

import java.sql.Timestamp;
import java.text.ParseException;

/**
 * Created by zhudebin on 14-7-10.
 */
public class SQLSERVERDataTypeConverter extends TDataTypeConverter {
    @Override
    Object convertData(TColumn tColumn, Object data) {
        switch (tColumn.getDbBaseDatatype()) {
            case TIMESTAMP:
                if (tColumn.getJdbcDataType() == TDataType.VARBINARY) {
                    switch (tColumn.getData_type()) {
                        case LONG:
                            return getLong((byte[]) data);
                        default:
                            throwException("timestampe类型转换不支持转换为" + tColumn.getData_type());
                    }
                } else {
                    throwException(" timestamp jdbc 只能支持 VARBINARY");
                }
                break;
            case ROWVERSION:
                if (tColumn.getJdbcDataType() == TDataType.VARBINARY) {
                    switch (tColumn.getData_type()) {
                        case LONG:
                            return getLong((byte[]) data);
                        default:
                            throwException("ROWVERSION" + tColumn.getData_type());
                    }
                } else {
                    throwException(" ROWVERSION jdbc 只能支持 VARBINARY");
                }
                break;
            case DATETIMEOFFSET:
                if (tColumn.getJdbcDataType() == TDataType.STRING) {
                    switch (tColumn.getData_type()) {
                        case TIMESTAMP:
                            try {
                                return new Timestamp(DateUtil.parse("yyyy-MM-dd HH:mm:ss.SSSSSSS", (String) data).getTime());
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                        default:
                            throwException("DATETIMEOFFSET 类型转换不支持转换为" + tColumn.getData_type());
                    }
                } else {
                    throwException(" DATETIMEOFFSET jdbc 只能支持 string");
                }
                break;
            case DATE:
                if (tColumn.getJdbcDataType() == TDataType.DATE) {
                    switch (tColumn.getData_type()) {
                        case TIMESTAMP:
                            return new Timestamp(((java.sql.Date)data).getTime());
                        case DATE:
                            return data;
                        default:
                            throwException("DATE类型转换不支持转换为" + tColumn.getData_type());
                    }
                } else {
                    throwException(" DATE jdbc 只能支持 DATE");
                }
                break;
            case REAL:
                if (tColumn.getJdbcDataType() == TDataType.FLOAT) {
                    switch (tColumn.getData_type()) {
                    case DOUBLE:
                        return Double.parseDouble(String.valueOf(data)) ;
                    default:
                        throwException("REAL类型转换不支持转换为" + tColumn.getData_type());
                    }
                } else {
                    throwException("REAL jdbc 只能支持 FLOAT");
                }
            break;
            default:
                throwException("source:" + tColumn.getJdbcDataType() + ",dist:" + tColumn.getData_type());
        }
        return null;
    }

    public static long getLong(byte[] buf) {
        if (buf == null) {
            throw new IllegalArgumentException("byte array is null!");
        }
        if (buf.length > 8) {
            throw new IllegalArgumentException("byte array size > 8 !");
        }
        long r = 0;
        for (byte a : buf) {
            r <<= 8;
            r |= (a & 0x00000000000000ff);
        }
        return r;
    }


}
