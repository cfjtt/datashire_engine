package com.eurlanda.datashire.engine.util.datatypeConverter;

import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by zhudebin on 14-7-10.
 */
public class OracleDataTypeConverter extends TDataTypeConverter {

    private static Log log = LogFactory.getLog(OracleDataTypeConverter.class);

    @Override
    Object convertData(TColumn tColumn, Object data) {
        switch (tColumn.getDbBaseDatatype()) {
            // long 类型数据存储的是文本类型，需要先用stream的方式获取流，然后取出成string
            case LONG:          // 配置文件设置为 binarystream
                if(tColumn.getJdbcDataType() == TDataType.BINARYSTREAM) {
                    InputStream is = (InputStream)data;
                    switch (tColumn.getData_type()) {
                        case STRING:
                            try {
                                return org.apache.commons.io.IOUtils.toString(is);
                            } catch (IOException e) {
                                log.error("oracle数据准换异常，long->string", e);
                                throw new RuntimeException(e);
                            }
                        default:
                            throw new RuntimeException("oracle数据抽取不存在该类型转换 long->" + tColumn.getData_type());
                    }
                } else {
                    throw new RuntimeException("oracle数据抽取不存在该jdbc类型转换 " + tColumn.getJdbcDataType());
                }
            // BINARY_FLOAT 类型，存在精度异常
            case BINARY_FLOAT:  // 配置文件设置为 float
                // float 类型
                if(tColumn.getJdbcDataType() == TDataType.FLOAT) {
                    switch (tColumn.getData_type()) {
                        case DOUBLE:
                        case FLOAT:
                            Float f = new Float(data+"");
                            return f;
//                            return new Double((float)data);
                        default:
                            throw new RuntimeException("oracle数据抽取不存在该类型转换 long->" + tColumn.getData_type());
                    }
                } else {
                    throw new RuntimeException("oracle数据抽取不存在该jdbc类型转换 " + tColumn.getJdbcDataType());
                }
            case BFILE:
                return null;
            default:
                throw new RuntimeException("oracle数据抽取不存在该类型转换 long->" + tColumn.getDbBaseDatatype());
        }
    }
}
