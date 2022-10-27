package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.enumeration.CDCSystemColumn;
import com.eurlanda.datashire.engine.exception.DataCannotConvertException;
import com.eurlanda.datashire.engine.exception.DataErrorException;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.DateUtil;
import com.eurlanda.datashire.engine.util.TransformationTypeAdaptor;
import com.eurlanda.datashire.engine.util.datatypeConverter.TDataTypeConverter;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import jodd.util.CsvUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.InputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;

public class TColumn implements Serializable {
    private static Log logger = LogFactory.getLog(TColumn.class);

    private static final long serialVersionUID = 1L;
    private int id;
    // 抽取后的列名
    private String name;
    // 抽取的列
    private String refName;

    // =============== 如果jdbc类型与数据库类型一致，则jdbcDataType 为空
    // extract jdbc抽取类型
    private TDataType jdbcDataType;
    // 抽取后的类型
    private TDataType data_type;
    // ===========================================================

    // 数据库字段类型
    private DbBaseDatatype dbBaseDatatype;

    private boolean nullable;
    private int length;
    private int precision;
    private int scale;
    private boolean isPrimaryKey = false;//数据库主键 可以多个并存
    private boolean isBusinessKey = false;//业务主键 可以多个并存
    // 是否为数据来源列 true:由transformation导出的，false:系统生成的
    private boolean isSourceColumn = true;
    // 抽取时间
    private Timestamp extractTime;
    // 落地 guid
    private boolean isGuid;

    // 落地数据库时才使用。
    private int cdc;

    public TColumn() {
    }

    public TColumn(TDataType data_type, String name, int id) {
        this.data_type = data_type;
        this.name = name;
        this.id = id;
    }

    public TColumn(TDataType data_type, int id) {
        this.data_type = data_type;
        this.id = id;
    }

    public TColumn(String name, int id, TDataType data_type) {
        this(data_type, id);
        this.name = name;
    }

    public TColumn(String name, int id, TDataType data_type, boolean nullable) {
        this(name, id, data_type);
        this.nullable = nullable;
    }

    public TColumn(String name, int id, TDataType data_type, boolean nullable, int cdc,
                   boolean isPrimaryKey, boolean isBusinessKey, boolean isSourceColumn) {
        this.data_type = data_type;
        this.id = id;
        this.name = name;
        this.cdc = cdc;
        this.nullable = nullable;
        this.isPrimaryKey = isPrimaryKey;
        this.isBusinessKey = isBusinessKey;
        this.isSourceColumn = isSourceColumn;
    }

    public TColumn(String name, int id, TDataType data_type, boolean nullable, boolean isPrimaryKey) {
        this(name, id, data_type);
        this.nullable = nullable;
        this.isPrimaryKey = isPrimaryKey;
    }

    public TColumn(int id, String name, TDataType data_type, boolean nullable, int length, int precision) {
        this(name, id, data_type);
        this.nullable = nullable;
        this.length = length;
        this.precision = precision;
    }

    public TColumn(int id, String name, String refName,
            TDataType data_type, boolean isSourceColumn) {
        this.id = id;
        this.name = name;
        this.refName = refName;
        this.data_type = data_type;
        this.isSourceColumn = isSourceColumn;
    }

    /**
     *
     * @param value
     * @param stmt
     * @param index
     * @param column
     * @throws SQLException
     * @since 2.0.0
     */
    public static void setValue(DataCell value,
            PreparedStatement stmt, Integer index, TColumn column) throws SQLException {
        if (value != null && value.getData() != null) {
            switch (value.getdType()) {
                case STRING:
                    stmt.setString(index, value.getData().toString());
                    break;
                case TINYINT:
                    stmt.setByte(index, (Byte)value.getData());
                    break;
                case SHORT:
                    stmt.setShort(index, Short.valueOf(value.getData().toString()));
                    break;
                case INT:
                    stmt.setInt(index, Integer.valueOf(String.valueOf(value.getData())));
                    break;
                case LONG:
                    stmt.setLong(index, Long.valueOf(String.valueOf(value.getData())));
                    break;
                case BIG_DECIMAL:
                    stmt.setBigDecimal(index, (BigDecimal) value.getData());
                    break;
                case FLOAT:
//                    stmt.setFloat(index, Float.valueOf(String.valueOf(value.getData())));
                    stmt.setFloat(index, Float.valueOf(value.getData()+""));
                    break;
                case DOUBLE:
//                    stmt.setDouble(index, Double.valueOf(String.valueOf(value.getData())));
                    stmt.setDouble(index, Double.valueOf(value.getData()+""));
                    break;
                case BOOLEAN:
                    stmt.setBoolean(index, (Boolean) value.getData());
                    break;
//                case TIME:
//                    stmt.setTime(index, (java.sql.Time) value.getData());
//                    break;
                case TIMESTAMP:
                    if(value.getData() instanceof  Timestamp) {
                        stmt.setTimestamp(index, (java.sql.Timestamp) value.getData());
                    } else if(value.getData() instanceof java.sql.Date) {
                        stmt.setTimestamp(index, DateUtil.util2Timestamp((java.sql.Date)value.getData()));
                    } else {
                        throw new RuntimeException("Tcolumn 数据落地时，不能对该数据类型"
                                + value.getData().getClass().getName() + " 插入timestamp类型");
                    }
                    break;
                case DATE:
                    stmt.setDate(index, (java.sql.Date) value.getData());
                    break;
                case VARBINARY:
                    stmt.setBytes(index, (byte[])value.getData());
                    break;
                case CSN:
                    stmt.setString(index, (String)value.getData());
                    break;
                // 对数组，MAP类型，落地的时候直接选择字符串 todo 还是说客户端做验证不准该类型落地
                case ARRAY:
                    stmt.setString(index, value.getData().toString());
                    break;
                case MAP:
                    stmt.setString(index, value.getData().toString());
                    break;
                case CSV:
                    stmt.setString(index, (String)value.getData());
                    break;
                default:
                    throw new RuntimeException("setValue 没有对应类型：" + value);
            }
        } else {
            switch (column.getData_type()) {
                case STRING:
                    stmt.setString(index, null);
                    break;
                case TINYINT:
                    stmt.setNull(index, Types.TINYINT);
                    break;
                case SHORT:
                    stmt.setNull(index, Types.SMALLINT);
                    break;
                case INT:
                    stmt.setNull(index, Types.INTEGER);
                    break;
                case LONG:
                    stmt.setNull(index, Types.BIGINT);
                    break;
                case BIG_DECIMAL:
//                    stmt.setNull(index, Types.DECIMAL);
                    stmt.setBigDecimal(index, null);
                    break;
                case FLOAT:
                    stmt.setNull(index, Types.FLOAT);
                    //                    stmt.setObject(index, null);
                    break;
                case DOUBLE:
                    stmt.setNull(index, Types.DOUBLE);
//                    stmt.setObject(index, null);
                    break;
                case BOOLEAN:
                    stmt.setNull(index, Types.BOOLEAN);
//                    stmt.setObject(index, null);
                    break;
//                case TIME:
////                    stmt.setNull(index, Types.TIME);
//                    stmt.setTime(index, null);
//                    break;
                case TIMESTAMP:
                    stmt.setTimestamp(index, null);
                    break;
                case DATE:
                    stmt.setDate(index, null);
                    break;
                case VARBINARY:
                    stmt.setBytes(index, null);
                    break;
                case CSN:
                    stmt.setString(index, null);
                    break;
                case CSV:
                    stmt.setString(index, null);
                    break;
                case MAP:
                case ARRAY:
                    stmt.setString(index,null);
                    stmt.setString(index,null);
                    break;
                default:
                    throw new RuntimeException("setValue 没有对应类型：" + value);
            }
        }

    }

    /**
     * 将根据tcolumn的数据库类型
     *
     * @param column
     * @param rs
     * @return
     * @throws SQLException
     */
    public static DataCell getValue(TColumn column, ResultSet rs) throws SQLException {
        return new DataCell(column.getData_type(), getValueFromRS(column.getData_type(), column.getName(), rs));
    }

    public static DataCell getValue(TDataTypeConverter tDataTypeConverter, TColumn column, ResultSet rs) throws SQLException {
        return new DataCell(column.data_type, tDataTypeConverter.convert(column,
                getValueFromRS(column.getJdbcDataType(), column.getName(), rs)));
    }

    private static Object getValueFromRS(TDataType tDataType, String columnName, ResultSet rs) throws SQLException {
        switch (tDataType) {
            case STRING:
                // 将 int的key,DataCell 添加进map中
                return rs.getString(columnName);
            case BIG_DECIMAL:
                return rs.getBigDecimal(columnName);
            case TINYINT:
                byte bt = rs.getByte(columnName);
                if(bt == 0) {
                    if(rs.getObject(columnName) == null) {
                        return null;
                    }
                }
                return bt;
            case SHORT:
                short sh = rs.getShort(columnName);
                if(sh == 0) {
                    if(rs.getObject(columnName) == null) {
                        return null;
                    }
                }
                return sh;
            case LONG:
                long lo = rs.getLong(columnName);
                if(lo == 0) {
                    if(rs.getObject(columnName) == null) {
                        return null;
                    }
                }
                return lo;
            case INT:
                // todo 优化这个查询
                int io = rs.getInt(columnName);
                if(io == 0) {
                    if(rs.getObject(columnName) == null) {
                        return null;
                    }
                }
                return io;
            case DOUBLE:
                double od = rs.getDouble(columnName);
                if(od == 0) {
                    if(rs.getObject(columnName) == null) {
                        return null;
                    }
                }
                return od;
            case FLOAT:
                float fo = rs.getFloat(columnName);
                if(fo == 0) {
                    if(rs.getObject(columnName) == null) {
                        return null;
                    }
                }
                return fo;
            case BOOLEAN:
                Boolean aBoolean = rs.getBoolean(columnName);
                if (!aBoolean) {
                    if (rs.getObject(columnName) == null) {
                        return null;
                    }
                }
                return aBoolean;
//            case TIME:
//                return rs.getTime(columnName);
            case DATE:
                return rs.getDate(columnName);
            case TIMESTAMP:
                if(rs.getObject(columnName)==null) {
                    return null;
                } else {
                    return rs.getTimestamp(columnName);
                }
            case VARBINARY:
                byte[] v = null;
                try {
                    v = rs.getBytes(columnName);
                } catch (Exception e){
                    logger.error("获取varbinary类型异常");
                }
                return v;
            case BINARYSTREAM:
                InputStream in = null;
                try {
                    in = rs.getBinaryStream(columnName);
                } catch (Exception e){
                    logger.error("获取binarystream异常");
                }
                return in;
            case CSV:
                return rs.getString(columnName);
            default:
                throw new RuntimeException("没有匹配的数据类型，请联系系统管理员，修改代码  CustomJavaSparkContext.scala  type:" + tDataType);
        }
    }

    public static Object toTColumnValue(String data, TColumn col) {
                return toTColumnValue(data, col.getData_type());
    }

    public static Object toTColumnValue(String data, TDataType tDataType) {
        Object obj = null;
        if (tDataType != TDataType.STRING && StringUtils.isBlank(data)) return null;
        if (tDataType != TDataType.STRING && "-".equals(data)) {
            return null;
        }
        if(data==null){
            return null;
        }
        try {
            switch (tDataType) {
                case STRING:
                    obj = data;
                    break;
                case TINYINT:
                    Double dt = Double.parseDouble(data);
                    if(dt != null) {
                        obj = dt.byteValue();
                    }
                    break;
                case SHORT:
                    Double s = Double.parseDouble(data);
                    if(s != null) {
                        obj = s.shortValue();
                    }
                    break;
                case INT:
                    Double d = Double.parseDouble(data);
                    if(d != null) {
                        obj = d.intValue();
                    }
                    break;
                case LONG:
                    if (data == null || "-".equals(data)) {
                        obj = null;
                    } else {
                        Double dl = Double.parseDouble(data);
                        if(dl != null) {
                            obj = dl.longValue();
                        }
                    }
                    break;
                case FLOAT:
                    Double df = Double.parseDouble(data);
                    if(df != null) {
                        obj = df.floatValue();
                    }
                    break;
                case DOUBLE:
                    obj = Double.valueOf(data);
                    break;
                case TIMESTAMP:
                    obj = TransformationTypeAdaptor.StringToTimestampWithNaso(data);
                    break;
//                case DATE:
//                    obj = defaultDateFormat.parse(data);
//                    break;
                case BOOLEAN:
                    obj = data.equalsIgnoreCase("true")
                            || data.equals("1")
                            || data.equalsIgnoreCase("y");
                    break;
                case BIG_DECIMAL:
                    obj = new BigDecimal(data);
                    break;
                case VARBINARY:
                    obj = data.getBytes();
                    break;
                case DATE:
                    obj = DateUtil.util2sql(DateUtil.parseByDefaultDateFormat(data));
                    break;
//                case TIME:
//                    obj = DateUtil.util2Time(DateUtil.parseByDefaultTimeFormat(data));
//                    break;
                case ARRAY:
                    obj=  TransformationTypeAdaptor.parseArray(data);
                    break;
                case MAP:
                    obj = TransformationTypeAdaptor.parseMap(data);
                    break;
                case CSV:
                    obj = data;
                    break;
                case CSN:
                    String[] strs = CsvUtil.toStringArray(data);
                    for(String str : strs) {
                        if(!org.apache.commons.lang3.StringUtils.isNumeric(str)) {  // 非数字
                            throw new EngineException("不能将非数字类型的字符串转换为CSN");
                        }
                    }
                    obj = data;
                    break;
                default:
                    throw new EngineException("文本抽取，不支持此类型");
            }
        } catch (Exception e) {
            logger.error(e.getMessage()+data+tDataType);
        }
        return obj;
    }

    public static Object convertBinary(byte[] bytes, TDataType tDataType) {
        try {
            switch (tDataType) {
            case STRING:
                return Bytes.toString(bytes);
            case BIG_DECIMAL:
                return Bytes.toBigDecimal(bytes);
            case SHORT:
                return Bytes.toShort(bytes);
            case LONG:
                return Bytes.toLong(bytes);
            case INT:
                return Bytes.toInt(bytes);
            case DOUBLE:
                return Bytes.toDouble(bytes);
            case FLOAT:
                return Bytes.toFloat(bytes);
            case BOOLEAN:
                return Bytes.toBoolean(bytes);
            case VARBINARY:
                return bytes;
            default:
                throw new DataErrorException("没有匹配的数据类型，请联系系统管理员，修改代码  TColumn  type:" + tDataType);
            }
        } catch (DataErrorException e) {
            throw e;
        } catch (Exception e) {
            throw new DataCannotConvertException("不能转换byte[] to " + tDataType.name());
        }
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public String getName(DataBaseType type) {
        return getName(type, name);
    }

    public  String getName(DataBaseType type, String name) {
        switch (type) {
            case MYSQL:
                return '`' + name + '`';
            case SQLSERVER:
                if(dbBaseDatatype == DbBaseDatatype.SQL_VARIANT && dbBaseDatatype!=null){
                    return "cast(["+name+"] as varchar)";
                } else {
                    return '[' + name + ']';
                }
            case ORACLE:
                return '"' + name + '"';
            case HBASE_PHOENIX:
                return '"' + name.toUpperCase() + '"';
            case DB2:
                return '"' + name + '"';
            case IMPALA:
                return '`' + name + '`';
            case TERADATA:
                return "\""+name+"\"";
            default:
                return name;
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public TDataType getData_type() {
        return data_type;
    }

    public void setData_type(TDataType data_type) {
        this.data_type = data_type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public boolean isSourceColumn() {
        return isSourceColumn;
    }

    public void setSourceColumn(boolean isSourceColumn) {
        this.isSourceColumn = isSourceColumn;
    }

    @Override
    public String toString() {
        return "TColumn{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", jdbcDataType=" + jdbcDataType +
                ", data_type=" + data_type +
                ", dbBaseDatatype=" + dbBaseDatatype +
                ", nullable=" + nullable +
                ", length=" + length +
                ", precision=" + precision +
                ", scale=" + scale +
                ", isPrimaryKey=" + isPrimaryKey +
                ", isBusinessKey=" + isBusinessKey +
                ", isSourceColumn=" + isSourceColumn +
                ", extractTime=" + extractTime +
                ", isGuid=" + isGuid +
                ", cdc=" + cdc +
                '}';
    }

    public int getCdc() {
        return cdc;
    }

    public void setCdc(int cdc) {
        this.cdc = cdc;
    }

    public boolean isBusinessKey() {
        return isBusinessKey;
    }

    public void setBusinessKey(boolean isBusinessKey) {
        this.isBusinessKey = isBusinessKey;
    }

    public boolean isCDCSystemColumn() {
        return this.getName().equals(CDCSystemColumn.DB_END_DATE_COL_NAME.getColValue()) ||
                this.getName().equals(CDCSystemColumn.DB_FLAG_COL_NAME.getColValue()) ||
                this.getName().equals(CDCSystemColumn.DB_START_DATE_COL_NAME.getColValue()) ||
                this.getName().equals(CDCSystemColumn.DB_VERSION_COL_NAME.getColValue());
    }

    public boolean isNotCDCSystemColumn() {
        return !isCDCSystemColumn();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TColumn other = (TColumn) obj;
        if (id != other.id)
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

    public DbBaseDatatype getDbBaseDatatype() {
        return dbBaseDatatype;
    }

    public void setDbBaseDatatype(DbBaseDatatype dbBaseDatatype) {
        this.dbBaseDatatype = dbBaseDatatype;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public TDataType getJdbcDataType() {
        return jdbcDataType;
    }

    public void setJdbcDataType(TDataType jdbcDataType) {
        this.jdbcDataType = jdbcDataType;
    }

    public Timestamp getExtractTime() {
        return extractTime;
    }

    public void setExtractTime(Timestamp extractTime) {
        this.extractTime = extractTime;
    }

    public boolean isGuid() {
        return isGuid;
    }

    public void setGuid(boolean isGuid) {
        this.isGuid = isGuid;
    }

    public String getRefName() {
        return refName;
    }

    public void setRefName(String refName) {
        this.refName = refName;
    }
}
