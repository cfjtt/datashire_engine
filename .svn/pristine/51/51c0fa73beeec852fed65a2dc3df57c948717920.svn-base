package com.eurlanda.datashire.engine.translation.extract;

import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.spark.DatabaseUtils;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SqlExtractManager extends ExtractManager{

    private TDataSource dataSource = null;
    private Connection conn = null;
    private final static String DATA_TYPE_NAME = "DATA_TYPE_NAME";
    public SqlExtractManager(TDataSource dataSource) {
        this.dataSource = dataSource;
        init();
    }

    @Override
    protected void init() {
        conn = DatabaseUtils.getConnection(dataSource);
    }

    @Override
    protected boolean isValidType(int incrementalMode) {
        String typeName = getDataType(dataSource);
        boolean isValid = true;
        switch (DbBaseDatatype.parse(typeName)){
            case TIMESTAMP:
                //最后修改时间
                if(dataSource.getType()==DataBaseType.SQLSERVER && incrementalMode==2){
                    isValid = false;
                }
                break;
            case IMAGE:
            case XML:
            case BINARY:
            case BFILE:
            case TEXT:
            case NCLOB:
            case BLOB:
            case LONGBLOB:
            case MEDIUMBLOB:
            case TINYBLOB:
            case CLOB:
            case RAW:
            case ROWID:
            case SDO_GEOMETRY:
            case SDO_TOPO_GEOMETRY:
            case UROWID:
            case XDBURITYPE:
            case DBURITYPE:
            case SDO_GEORASTER:
            case URITYPE:
            case XMLTYPE:
            case LONG:
            case GRAPHIC:
            case GEOGRAPHY:
            case DBCLOB:
            case GEOMETRY:
            case HIERARCHYID:
                isValid = false;
                break;
            default:
                break;
        }
        return isValid;
    }

    @Override
    protected String getMaxValueByColumn(String tableName, String columnName) {
        String maxValue = null;
        String typeName="";
        try {
            String sql = "select max("+columnName+") from "+tableName;
            //oracle特殊类型的使用max出错
            if(dataSource.getType()==DataBaseType.ORACLE){
                typeName = getDataType(dataSource);
                if(DbBaseDatatype.parse(typeName)==DbBaseDatatype.CLOB
                        || DbBaseDatatype.parse(typeName)==DbBaseDatatype.NCLOB
                        || DbBaseDatatype.parse(typeName)== DbBaseDatatype.BLOB){
                    sql = "select max(to_char("+columnName+")) from "+tableName;
                }
            }
            PreparedStatement statement = conn.prepareStatement(sql);
            ResultSet rs = statement.executeQuery();
            if(StringUtils.isEmpty(typeName)) {
                typeName = rs.getMetaData().getColumnTypeName(rs.getMetaData().getColumnCount()).toUpperCase().replaceAll("\\s", "").replaceAll("\\([0-9]+,?[0-9]*\\)","");
            }
            maxValue = getMaxValue(typeName,rs);
            setFilterInternalData(DATA_TYPE_NAME, typeName);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return maxValue;
    }

    private String getMaxValue(String typeName,ResultSet rs) throws Exception {
        String maxValue = "";
        try {
            while (rs.next()) {
                switch (DbBaseDatatype.parse(typeName)) {
                    case BINARY:
                    case BLOB:
                    case CLOB:
                    case NCLOB:
                    case RAW:
                    case LONGRAW:
                    case TINYBLOB:
                    case MEDIUMBLOB:
                    case LONGBLOB:
                    case DBCLOB:
                    case GRAPHIC:
                    case IMAGE:
                    case VARBINARY:
                        InputStream reader = rs.getBinaryStream(1);
                        if (reader != null) {
                            byte[] buffer = new byte[reader.available()];
                            StringBuilder builder = new StringBuilder("");
                            int n = 0;
                            while ((n = reader.read(buffer)) > 0) {
                                builder.append(new String(buffer, 0, n));
                            }
                            maxValue = builder.toString();
                        } else {
                            maxValue = "null";
                        }
                        break;
                    case TIMESTAMP:
                    case DATE:
                        String format = "yyyy-MM-dd HH:mm:ss";
                        if(DbBaseDatatype.parse(typeName) == DbBaseDatatype.DATE){
                            format = "yyyy-MM-dd";
                            if(DataBaseType.ORACLE == dataSource.getType()) {
                                format = "yyyy-MM-dd HH:mm:ss";
                            }
                        }
                        if(dataSource.getType()==DataBaseType.SQLSERVER && DbBaseDatatype.parse(typeName) == DbBaseDatatype.TIMESTAMP){
                            maxValue = rs.getInt(1)+"";
                        } else {
                            SimpleDateFormat dateFormat = new SimpleDateFormat(format);
                            Timestamp v = rs.getTimestamp(1);
                            if (v != null) {
                                maxValue = dateFormat.format(rs.getTimestamp(1));
                            } else {
                                maxValue = null;
                            }
                        }
                        //maxValue = rs.getTimestamp(1).toString();
                        break;
                    default:
                        maxValue = rs.getString(1);
                        break;
                }
            }
        } catch (Exception e){
            throw e;
        } finally {
            if(rs!=null){
                rs.close();
            }
        }
        return maxValue;
    }
    @Override
    protected String getMaxValueByColumn(String[] tableNames, String columnName,String lastValue,int limit) {
        String maxValue = null;
        String typeName="";
        try {
            if(tableNames.length>0) {
                StringBuffer sqlBuffer = new StringBuffer("");
                for (int i = 0; i < tableNames.length; i++) {
                    String tableName = tableNames[i];
                    if (limit > 0) {
                        //oracle特殊类型的使用max出错
                        if(dataSource.getType()==DataBaseType.ORACLE){
                            typeName = getDataType(dataSource);
                            if(DbBaseDatatype.parse(typeName)==DbBaseDatatype.CLOB
                                    || DbBaseDatatype.parse(typeName)==DbBaseDatatype.NCLOB
                                    || DbBaseDatatype.parse(typeName)== DbBaseDatatype.BLOB){
                                if(com.eurlanda.datashire.utility.StringUtils.isNotNull(lastValue)){
                                    sqlBuffer.append("(select v as v1  from ( select * from (select v,ROWNUM as no from (select v from (select "+escapeColName(columnName)+" as v  from "+tableName+" where "+escapeColName(columnName)+" > "+lastValue+" and "+columnName+" is not null order by "+escapeColName(columnName)+")) ) a where a.no>0 and a.no<="+limit+") a_"+i+")");
                                } else {
                                    sqlBuffer.append("(select v as v1  from ( select * from (select v,ROWNUM as no from (select v from (select "+escapeColName(columnName)+" as v  from "+tableName+" where "+columnName+" is not null order by "+escapeColName(columnName)+")) ) a where a.no>0 and a.no<="+limit+") a_"+i+")");
                                }
                            } else {
                                if(com.eurlanda.datashire.utility.StringUtils.isNotNull(lastValue)){
                                    String v = convertValueStrToExpString(lastValue);
                                    sqlBuffer.append("(select v as v1  from ( select * from (select v,ROWNUM as no from (select "+escapeColName(columnName)+" as v  from "+tableName+" where "+escapeColName(columnName)+" > "+v+"  and "+columnName+" is not null order by "+escapeColName(columnName)+" ) ) a where a.no>0 and a.no<="+limit+") a_"+i+")");
                                } else {
                                    sqlBuffer.append("(select v as v1  from ( select * from (select v,ROWNUM as no from (select "+escapeColName(columnName)+" as v  from "+tableName+" where "+columnName+" is not null order by "+escapeColName(columnName)+" ) ) a where a.no>0 and a.no<="+limit+") a_"+i+")");
                                }
                            }
                        } else if(dataSource.getType()==DataBaseType.MYSQL){
                            if(com.eurlanda.datashire.utility.StringUtils.isNotNull(lastValue)){
                                String v = convertValueStrToExpString(lastValue);
                                sqlBuffer.append("(select v as v1 from (select (" + escapeColName(columnName)+ ") as v from " + tableName + " where "+escapeColName(columnName)+" > "+v+" and "+columnName+" is not null order by " + escapeColName(columnName) + " limit 0," + limit+") b_"+i+") ");
                            } else {
                                sqlBuffer.append("(select v as v1 from (select (" + escapeColName(columnName) + ") as v from " + tableName + " where "+columnName+" is not null order by " + escapeColName(columnName) + " limit 0," + limit+") b_"+i+") ");
                            }
                        } else if(dataSource.getType()==DataBaseType.DB2){
                            // fetch first "+this.maxCount+" rows only
                            if(com.eurlanda.datashire.utility.StringUtils.isNotNull(lastValue)){
                                String v = convertValueStrToExpString(lastValue);
                                String sql = "(select col_name as v1 from (select * from (select col_name,rownumber() over (order by col_name) as no from (select "+escapeColName(columnName)+" as col_name from "+tableName+" where "+escapeColName(columnName)+">"+v+" and "+columnName+" is not null )) a where a.no between 0 and "+limit+") b_"+i+")";
                                sqlBuffer.append(sql);
                            } else {
                                String sql = "(select col_name as v1 from (select * from (select col_name,rownumber() over (order by col_name) as no from (select "+escapeColName(columnName)+" as col_name from "+tableName+" where "+columnName+" is not null )) a where a.no between 0 and "+limit+") b_"+i+")";
                                sqlBuffer.append(sql);
                            }
                        } else if(dataSource.getType()==DataBaseType.TERADATA || dataSource.getType()==DataBaseType.SQLSERVER){
                            //top
                            if(com.eurlanda.datashire.utility.StringUtils.isNotNull(lastValue)){
                                String v = convertValueStrToExpString(lastValue);
                                String sql ="(select "+escapeColName(columnName)+" as v1 from (select top "+limit+" "+columnName+" from "+tableName+" where "+escapeColName(columnName)+" > "+v+" and "+columnName+" is not null order by "+escapeColName(columnName)+") b_"+i+")";
                                sqlBuffer.append(sql);
                            } else {
                                String sql ="(select "+escapeColName(columnName)+" as v1 from (select top "+limit+" "+columnName+" from "+tableName+"  where "+columnName+" is not null order by "+escapeColName(columnName)+") b_"+i+")";
                                sqlBuffer.append(sql);
                            }
                        }
                    } else {
                        if(dataSource.getType()==DataBaseType.ORACLE){
                            typeName = getDataType(dataSource);
                            if(DbBaseDatatype.parse(typeName)==DbBaseDatatype.CLOB
                                    || DbBaseDatatype.parse(typeName)==DbBaseDatatype.NCLOB
                                    || DbBaseDatatype.parse(typeName)== DbBaseDatatype.BLOB){
                                if(com.eurlanda.datashire.utility.StringUtils.isNotNull(lastValue)){
                                    sqlBuffer.append("(select "+escapeColName(columnName)+" as v1 from " + tableName + " where "+escapeColName(columnName)+" > "+lastValue+") ");
                                } else {
                                    sqlBuffer.append("(select "+escapeColName(columnName)+" as v1 from " + tableName + ")");
                                }
                            } else {
                                if(com.eurlanda.datashire.utility.StringUtils.isNotNull(lastValue)){
                                    String v = convertValueStrToExpString(lastValue);
                                    sqlBuffer.append("(select " + escapeColName(columnName) + " as v1 from " + tableName + " where "+escapeColName(columnName)+" > "+v+")");
                                } else {
                                    sqlBuffer.append("(select " + escapeColName(columnName) + " as v1 from " + tableName + ")");
                                }
                            }
                        } else {
                            if(com.eurlanda.datashire.utility.StringUtils.isNotNull(lastValue)){
                                String v = convertValueStrToExpString(lastValue);
                                sqlBuffer.append("(select " + escapeColName(columnName) + " as v1 from " + tableName+" where "+escapeColName(columnName)+" > "+v+")");
                            } else {
                                sqlBuffer.append("(select " + escapeColName(columnName) + " as v1 from " + tableName + ")");
                            }
                        }
                    }
                    if (tableNames.length > 1 && i < tableNames.length - 1) {
                        sqlBuffer.append(" union all");
                    }
                }

                String sql = "select max(v1) from ("+sqlBuffer.toString()+") c";
                if(dataSource.getType() == DataBaseType.SQLSERVER && DbBaseDatatype.parse(getDataType(dataSource))==DbBaseDatatype.TIMESTAMP){
                    sql = "select max(v1) from (select cast(v1 as BIGINT) as v1 from ("+sqlBuffer.toString()+") a) c";
                }
                if(limit>0){
                    if(dataSource.getType()==DataBaseType.ORACLE){
                        typeName = getDataType(dataSource);
                        if(DbBaseDatatype.parse(typeName)==DbBaseDatatype.CLOB
                                || DbBaseDatatype.parse(typeName)==DbBaseDatatype.NCLOB
                                || DbBaseDatatype.parse(typeName)== DbBaseDatatype.BLOB){
                            sql = "select max(v1) from (select v1 from (select v1,ROWNUM as no from (select v1  from ( "+sqlBuffer.toString()+") order by v1 )) where no>0 and no<="+limit+" ) c";
                        } else {
                            sql = "select max(v1) from (select v1 from (select v1,ROWNUM as no from ( select v1 from ("+sqlBuffer.toString()+") order by v1 )) where no>0 and no<="+limit+" ) c";

                        }
                    } else if(dataSource.getType()==DataBaseType.MYSQL){
                            sql = "select max(v1) from (select * from ("+sqlBuffer.toString()+") abc order by v1 limit 0,"+limit+"  ) c";
                    } else if(dataSource.getType()==DataBaseType.DB2){
                            sql = "select max(v1) from (select v1 from (select v1,rownumber() over (order by v1) as no from ("+sqlBuffer.toString()+")) where no>0 and no<="+limit+")";
                    } else if(dataSource.getType()==DataBaseType.TERADATA || dataSource.getType()==DataBaseType.SQLSERVER){
                        //top
                        //sql="select max(v1) from (select top "+limit+" v1 from ("+sqlBuffer.toString()+") order by v1)";
                        typeName = getDataType(dataSource);
                        if(dataSource.getType() == DataBaseType.SQLSERVER && DbBaseDatatype.parse(typeName)==DbBaseDatatype.TIMESTAMP){
                            sql = "select max(v1) from ( select top " + limit + " cast(v1 as BIGINT) as v1 from (" + sqlBuffer.toString() + ") a order by v1) b";
                        } else {
                            sql = "select max(v1) from ( select top " + limit + " v1 from (" + sqlBuffer.toString() + ") a order by v1) b";
                        }
                    }
                }
                PreparedStatement statement = conn.prepareStatement(sql);
                ResultSet rs = statement.executeQuery();
                if(StringUtils.isEmpty(typeName)) {
                    typeName = rs.getMetaData().getColumnTypeName(rs.getMetaData().getColumnCount()).toUpperCase().replaceAll("\\s", "").replaceAll("\\([0-9]+,?[0-9]*\\)","");
                }
                maxValue = getMaxValue(typeName,rs);
                setFilterInternalData(DATA_TYPE_NAME, typeName);
            }
        } catch (Exception e1) {
            e1.printStackTrace();
            logger.error(e1.getMessage());
        }
        return maxValue;
    }

    @Override
    protected String getCurrentDsTimestamp() {
        String sql = null;
        switch (dataSource.getType()){
            case SQLSERVER:
                sql = "select getdate()";
                break;
            case ORACLE:
                sql = "select to_char(sysdate,'yyyy-MM-dd HH24:mi:ss') from dual";
                break;
            case DB2:
                sql = "select current timestamp from sysibm.sysdummy1";
                break;
            case INFORMIX:
                sql = "select sysdate from  sysmaster:sysshmvals";
                break;
            case MYSQL:
                sql = "select now()";
                break;
            case TERADATA:
                sql = "select current_timestamp";
                break;
            case HBASE_PHOENIX:
                sql = "select current_time() from "+dataSource.getTableName();
                break;
            case HANA:
                sql = "select CURRENT_TIMESTAMP  from DUMMY";
                break;
            default:
                new RuntimeException("不支持的数据库类型");
        }
        try {
            PreparedStatement statement = conn.prepareStatement(sql);
            ResultSet rs = statement.executeQuery();
            String time = null;
            while(rs.next()){
                time = rs.getString(1);
            }
            return time;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected String convertValueStrToExpString(String valueStr) {
        StringBuilder filterStr = new StringBuilder();
        String tableName = (String)getFilterInternalData(TABLE_NAME);
        String columnName = (String)getFilterInternalData(COLUMN_NAME);
        String type = getDataType(dataSource);

        switch (DbBaseDatatype.parse(type)) {
            case BIGINT:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case INT:
            case SMALLINT:
            case TINYINT:
            case LONG:
            case INTEGER:
            case UNSIGNED_INT:
            case UNSIGNED_FLOAT:
            case UNSIGNED_LONG:
            case UNSIGNED_DOUBLE:
            case UNSIGNED_TINYINT:
            case UNSIGNED_SMALLINT:
            case MONEY:
            case SMALLMONEY:
            case NUMERIC:
            case NUMBER:
            case SMALLDECIMAL:
            case ROWVERSION:
            case BINARY_DOUBLE:
            case BINARY_FLOAT:
            case DECFLOAT:
            case BYTEINT:
            case INTERVAL:
            case INTERVALDAY:
            case INTERVALDAYTOHOUR:
            case INTERVALDAYTOMINUTE:
            case INTERVALDAYTOSECOND:
            case INTERVALHOUR:
            case INTERVALHOURTOMINUTE:
            case INTERVALHOURTOSECOND:
            case INTERVALMINUTE:
            case INTERVALMINUTETOSECOND:
            case INTERVALMONTH:
            case INTERVALSECOND:
            case INTERVALYEAR:
            case INTERVALYEARTOMONTH:
                filterStr.append(valueStr);
                break;
            case REAL:
                filterStr.append("cast("+valueStr+" as real)");
            case DATE:
            case DATETIME:
            case TIMESTAMP:
            case DATETIME2:
            case SMALLDATETIME:
            case UNSIGNED_TIME:
            case UNSIGNED_DATE:
            case UNSIGNED_TIMESTAMP:
            case SECONDDATE:
            case TIME:
                String format;
                if(dataSource.getType()==DataBaseType.SQLSERVER && DbBaseDatatype.parse(type)==DbBaseDatatype.TIMESTAMP){
                    //当做int类型处理
                    filterStr.append(valueStr);
                    break;
                }
                if(DbBaseDatatype.parse(type)==DbBaseDatatype.DATE){
                    format = "yyyy-MM-dd";
                    if(DataBaseType.ORACLE == dataSource.getType()) {
                        format = "yyyy-MM-dd HH:mm:ss";
                    }
                } else if(DbBaseDatatype.parse(type)==DbBaseDatatype.SMALLDATETIME){
                    format="yyyy-MM-dd HH:mm";
                } else if(DbBaseDatatype.parse(type)==DbBaseDatatype.TIME){
                    if(com.eurlanda.datashire.utility.StringUtils.isNotNull(valueStr) && valueStr.contains(".")) {
                        int length = valueStr.split("\\.")[1].length();
                        StringBuffer hmaoBuffer = new StringBuffer("");
                        for(int i=0;i<length;i++){
                            hmaoBuffer.append("S");
                        }
                        format = "HH:mm:ss."+hmaoBuffer.toString();
                    } else {
                        format = "HH:mm:ss";
                    }
                } else if(DbBaseDatatype.parse(type)== DbBaseDatatype.TIMESTAMP
                        || DbBaseDatatype.parse(type) == DbBaseDatatype.DATETIME){
                    if(com.eurlanda.datashire.utility.StringUtils.isNotNull(valueStr) && valueStr.contains(".")) {
                        int length = valueStr.split("\\.")[1].length();
                        StringBuffer hmaoBuffer = new StringBuffer("");
                        for(int i=0;i<length;i++){
                            hmaoBuffer.append("S");
                        }
                        format = "yyyy-MM-dd HH:mm:ss."+hmaoBuffer.toString();
                    } else {
                        format = "yyyy-MM-dd HH:mm:ss";
                    }
                } else {
                    format = "yyyy-MM-dd HH:mm:ss";
                }
                //将输入的类型先转换成date类型，然后在转换成str
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
                try {
                    if(com.eurlanda.datashire.utility.StringUtils.isNotNull(valueStr)) {
                        Date date = simpleDateFormat.parse(valueStr);
                        if(DbBaseDatatype.parse(type) != DbBaseDatatype.TIME){
                            valueStr = simpleDateFormat.format(date);
                        }
                        if (dataSource.getType() == DataBaseType.ORACLE) {
                            //oracle不区分大小写，所以时间转换格式要特殊处理
                            if (DbBaseDatatype.parse(type) == DbBaseDatatype.TIME) {
                                format = "HH24:mi:ss";
                            } else {
                                format = "yyyy-MM-dd HH24:mi:ss";
                            }
                            filterStr.append("to_date('").append(valueStr).append("',").append("'").append(format).append("')");
                        } else {
                            filterStr.append("'").append(valueStr).append("'");
                        }
                    } else {
                        filterStr.append(valueStr);
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                    throw  new RuntimeException("时间类型格式不正确:"+valueStr+" 应该为:"+format);
                }
                break;
            default:
                //默认按字符串进行处理
                //因为使用json转换，去掉首尾的"
                if(valueStr==null){
                    valueStr = "null";
                }
                if(valueStr.startsWith("\"") && valueStr.endsWith("\"")){
                    valueStr = valueStr.substring(1,valueStr.length()-1);
                }
                valueStr = valueStr.replaceAll("'","''");
                if(dataSource.getType() == DataBaseType.SQLSERVER){
                    if(com.eurlanda.datashire.utility.StringUtils.isHavaChinese(valueStr)){
                        filterStr.append("N");
                    }
                }
                filterStr.append("'").append(valueStr).append("'");
                break;
        }
        return filterStr.toString();
    }

    @Override
    public void close() {
        if(conn!=null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private String getDataType(TDataSource dataSource) {
        String typeName = DbBaseDatatype.parse(dataSource.getCheckColumnType()).name();
        return typeName;
    }

    @Override
    protected String escapeColName(String colName) {
        if(dataSource.getType()==DataBaseType.ORACLE){
            String typeName = getDataType(dataSource);
            if(DbBaseDatatype.parse(typeName) == DbBaseDatatype.BLOB
                    || DbBaseDatatype.parse(typeName)==DbBaseDatatype.CLOB
                    || DbBaseDatatype.parse(typeName) == DbBaseDatatype.NCLOB){
                return "to_char("+colName+")";
            }
        }
        if(dataSource.getType() == DataBaseType.SQLSERVER){
            String typeName = getDataType(dataSource);
            if(DbBaseDatatype.parse(typeName) == DbBaseDatatype.NTEXT
                    || DbBaseDatatype.parse(typeName) == DbBaseDatatype.TEXT){
                return "cast("+colName+" as nvarchar(max))";
            }
        }
        return super.escapeColName(colName);
    }

    @Override
    protected DataBaseType getDbType() {
        return dataSource.getType();
    }
}
