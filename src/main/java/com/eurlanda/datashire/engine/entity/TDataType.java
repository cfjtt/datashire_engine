package com.eurlanda.datashire.engine.entity;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;

/**
 * squid flow 中会存在的数据类型，其中 KEY为非常量类型，表示数据集中的key
 *
 * 数值类型:        --> Spark SQL 类型
 *  1. TINYINT          -->     ByteType: Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
 *  2. SHORT            -->     ShortType: Represents 2-byte signed integer numbers. The range of numbers is from -32768 to 32767.
 *  3. INT              -->     IntegerType: Represents 4-byte signed integer numbers. The range of numbers is from -2147483648 to 2147483647.
 *  4. LONG             -->     LongType: Represents 8-byte signed integer numbers. The range of numbers is from -9223372036854775808 to 9223372036854775807.
 *  5. FLOAT            -->     FloatType: Represents 4-byte single-precision floating point numbers.
 *  6. DOUBLE           -->     DoubleType: Represents 8-byte double-precision floating point numbers.
 *  7. BIG_DECIMAL      -->     DecimalType: Represents arbitrary-precision signed decimal numbers.
 *                              Backed internally by java.math.BigDecimal.
 *                              A BigDecimal consists of an arbitrary precision integer unscaled value and a 32-bit integer scale.
 * 字符类型:
 *  1. STRING           -->     StringType: Represents character string values.
 *  2. CSV
 *  3. CSN
 *
 * 二进制类型:
 *  1. VARBINARY        -->     BinaryType: Represents byte sequence values.
 *
 * 布尔类型:
 *  1. BOOLEAN          -->     BooleanType: Represents boolean values.
 *
 * 时间类型:
 *  1. DATE             -->     DateType: Represents values comprising values of fields year, month, day.
 *  2. TIMESTAMP        -->     TimestampType: Represents values comprising values of fields year, month, day, hour, minute, and second.
 *
 * 复杂类型:
 *  1. ARRAY
 *  2. MAP
 *  3. LABELED_POINT
 *  4. RATING
 * @author zhudebin
 */
public enum TDataType {
    //base type
    TINYINT(27),
    SHORT(26),
    INT(1),
    LONG(3),
    FLOAT(6),
    DOUBLE(4),
    BIG_DECIMAL(9),
    STRING(2),
    BOOLEAN(5),
//    TIME(7),    // 该类型不需要了
    DATE(25),
    TIMESTAMP(8),
    // byte[]
    VARBINARY(10),
    BINARYSTREAM(11),

    //special type
    ARRAY(86),
    MAP(1022),
    // 训练数据类型
    LABELED_POINT(10021),
    // 表示该数据是 数据集中的某一列
    TCOLUMN(10022),
    CSN(10023), //  "1,2,3,4,5,6"
    // 协同过滤，（用户ID:Int,物品ID:Int，打分:Double）
    RATING(10024),
    CSV(10025);

    private int dataType;

    TDataType(int dataType) {
        this.dataType = dataType;
    }

    public static java.sql.Timestamp newTimestamp(){
        return new java.sql.Timestamp(System.currentTimeMillis());
    }

    public int getDataType() {
        return dataType;
    }

    public static TDataType convert(Integer i) {
        for (TDataType tdt : TDataType.values()) {
            if (tdt.dataType == i) {
                return tdt;
            }
        }
        throw new RuntimeException("没有此类型，不能转化");
    }

    /**
     * 将后台数据类型转换为TdataType.
     *
     * @param sys_datatype 后台数据类型的int形式。
     * @return
     */
    public static TDataType sysType2TDataType(int sys_datatype) {
        if (sys_datatype == SystemDatatype.BIGINT.value()) {
            return TDataType.LONG;
        } else if (sys_datatype == SystemDatatype.INT.value()) {
            return TDataType.INT;
        } else if (sys_datatype == SystemDatatype.SMALLINT.value()) {
            return TDataType.SHORT;
        } else if (sys_datatype == SystemDatatype.TINYINT.value()) {
            return TDataType.TINYINT;
        } else if (sys_datatype == SystemDatatype.NVARCHAR.value()
                || sys_datatype == SystemDatatype.NCHAR.value()
                || sys_datatype == SystemDatatype.CSN.value()) {
            return TDataType.STRING;
        } else if (sys_datatype == SystemDatatype.FLOAT.value()) {
            return TDataType.FLOAT;
        } else if (sys_datatype == SystemDatatype.DOUBLE.value()) {
            return TDataType.DOUBLE;
        } else if (sys_datatype == SystemDatatype.BIT.value()) {
            return TDataType.BOOLEAN;
        } else if (sys_datatype == SystemDatatype.DECIMAL.value()) {
            return TDataType.BIG_DECIMAL;
        } else if (sys_datatype == SystemDatatype.DATETIME.value()) {
            return TDataType.TIMESTAMP;
        } else if (sys_datatype == SystemDatatype.DATE.value()) {
            return TDataType.DATE;
        } else if (sys_datatype == SystemDatatype.VARBINARY.value()
                || sys_datatype == SystemDatatype.BINARY.value()) {
            return TDataType.VARBINARY;
        } else if(sys_datatype == SystemDatatype.ARRAY.value()) {
            return TDataType.ARRAY;
        } else if(sys_datatype == SystemDatatype.MAP.value()) {
            return TDataType.MAP;
        } else if(sys_datatype == SystemDatatype.CSV.value()) {
            return TDataType.CSV;
        }
        else if (sys_datatype == 0) {
            throw new RuntimeException("ColumnType=OTHER，请先确定数据类型");
        }
        throw new RuntimeException("不受支持的数据类型：" + sys_datatype);
    }

    /**
     *
     * @param dtStr
     * @return
     */
    public static TDataType str2TDataType(String dtStr) {
    	if(ValidateUtils.isEmpty(dtStr)){
    		return null;
    	}
        switch (dtStr)  {
            case "long":
                return TDataType.LONG;
            case "int":
                return TDataType.INT;
            case "string":
                return TDataType.STRING;
            case "double":
                return TDataType.DOUBLE;
            case "float":
                return TDataType.FLOAT;
            case "boolean":
                return TDataType.BOOLEAN;
            case "byte[]":
                return TDataType.VARBINARY;
//            case "time":
//                return TDataType.TIME;
            case "date":
                return TDataType.DATE;
            case "timestamp":
                return TDataType.TIMESTAMP;
            case "bigdecimal":
                return TDataType.BIG_DECIMAL;
            case "binarystream":
                return TDataType.BINARYSTREAM;
            default:
                throw new RuntimeException("不支持该字符串转换为TDataType:" + dtStr);
        }
    }
}

