package com.eurlanda.datashire.engine.translation.extract;

import com.datastax.driver.core.*;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.enumeration.DataBaseType;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by zhudebin on 2017/5/10.
 */
public class CassandraExtractManager extends ExtractManager {

    private Session session;

    private String[] host;
    private int port;
    private int verification;
    private String username;
    private String password;
    private String keyspace;
    private String clusterName;

    private final static String DATA_TYPE_NAME = "DATA_TYPE_NAME";

    public CassandraExtractManager(String[] host,int port,int verification,String username,String password,String keyspace,String clusterName) {
        this.host = host;
        this.port = port;
        this.verification = verification;
        this.username = username;
        this.password = password;
        this.keyspace = keyspace;
        this.clusterName = clusterName;
        init();
    }

    @Override
    protected void init() {
        try {
            PoolingOptions poolingOptions = new PoolingOptions();
            poolingOptions.setConnectionsPerHost(HostDistance.REMOTE,2,10)
                    .setConnectionsPerHost(HostDistance.LOCAL,2,10);
            Cluster cluster;
            if (port >= 0) {
                if (verification == 1) {
                    cluster = Cluster.builder().addContactPoints(host).withPort(port).withClusterName(clusterName).withPoolingOptions(poolingOptions).withCredentials(username, password).build();
                } else {
                    cluster = Cluster.builder().addContactPoints(host).withPort(port).withClusterName(clusterName).withPoolingOptions(poolingOptions).build();
                }

            } else {
                if (verification == 1) {
                    cluster = Cluster.builder().addContactPoints(host).withClusterName(clusterName).withCredentials(username, password).withPoolingOptions(poolingOptions).build();
                } else {
                    cluster = Cluster.builder().addContactPoints(host).withClusterName(clusterName).withPoolingOptions(poolingOptions).build();
                }
            }
            if (!cluster.getMetadata().getClusterName().equals(clusterName)) {
                throw new EngineException("cassandra cluster不存在");
            }
            session = cluster.connect(keyspace);
        } catch (Exception e){
            logger.error("初始化Cassandra异常", e);
            throw e;
        }
    }

    @Override protected String getMaxValueByColumn(String tableName, String columnName) {
        StringBuilder sqlBuilder = new StringBuilder("select tojson(max(");
        sqlBuilder.append(escapeColName(columnName))
                .append(")) from ")
                .append(escapeTableName(tableName));
        ResultSet rs = session.execute(sqlBuilder.toString());
        String maxValue = rs.one().getString(0);
        /** 通过tojson转成字符串  */
        DataType.Name type = getDataType(tableName, columnName);
        // 数字类型没有双引号
        switch (type) {
        case BIGINT:
        case COUNTER:
        case DECIMAL:
        case DOUBLE:
        case FLOAT:
        case INT:
        case SMALLINT:
        case TINYINT:
        case VARINT:
            return maxValue;
        }
        /**
        switch (type) {
        case ASCII:
        case TEXT:
        case TIMEUUID:
        case VARCHAR:
        case UUID:
        case DATE:
        case TIMESTAMP:
            return maxValue.substring(1, maxValue.length()-1);
        }  */

        return maxValue.substring(1, maxValue.length()-1);
    }

    @Override protected String getCurrentDsTimestamp() {
        ResultSet rs = session.execute("select tojson(totimestamp(now())) from system_schema.keyspaces limit 1");
        Row row = rs.one();
        /**
        Date date = row.getTimestamp(0);
        DataType.Name type = getDataType(null, null);
        String formatStr = null;
        if(type == DataType.Name.DATE) {
            formatStr = "yyyy-MM-dd";
        } else if(type == DataType.Name.TIMESTAMP) {
            formatStr = "yyyy-MM-dd HH:mm:ssZ";
        } else {
            throw new EngineException("[最后修改时间]抽取方式不能使用该类型列," + type.name());
        }
        SimpleDateFormat parser = new SimpleDateFormat(formatStr);
        parser.setLenient(false);
        // set a default timezone for patterns that do not provide one
        parser.setTimeZone(TimeZone.getTimeZone("UTC"));
        return parser.format(date);
         */
        String maxTimestamp = row.getString(0);
        DataType.Name type = getDataType(null, null);
        if(type == DataType.Name.DATE) {
            return maxTimestamp.substring(1, maxTimestamp.length() -1).split("\\s")[0];
        } else if(type == DataType.Name.TIMESTAMP) {
            return maxTimestamp.substring(1, maxTimestamp.length() -1);
        } else {
            throw new EngineException("[最后修改时间]抽取方式不能使用该类型列," + type.name());
        }
    }

    @Override protected String convertValueStrToExpString(String valueStr) {
        StringBuilder filterStr = new StringBuilder();
        String tableName = (String)getFilterInternalData(TABLE_NAME);
        String columnName = (String)getFilterInternalData(COLUMN_NAME);
        DataType.Name type = getDataType(tableName, columnName);
        switch (type) {
        case ASCII:
        case TEXT:
        case VARCHAR:
            filterStr.append("'").append(valueStr).append("'"); // <=
            break;
        case BIGINT:
        case COUNTER:
        case DECIMAL:
        case DOUBLE:
        case FLOAT:
        case INT:
        case SMALLINT:
        case TINYINT:
        case VARINT:
            filterStr.append(valueStr); // <=
            break;
        case DATE:
        case TIMESTAMP:
            filterStr.append("'").append(valueStr).append("'");
            break;
//        case TIMEUUID:    TODO 没有函数可以将字符串转换为timeuuid类型
//            filterStr.append("totimestamp('").append(valueStr).append("')");
//            break;
        default:
            throw new EngineException("不支持cassandra增量抽取列不支持该类型:" + type);
        }

        return filterStr.toString();
    }

    @Override
    protected boolean isValidType(int incrementalMode) {
        return false;
    }

    /**
     * 一个manager只能计算一个抽取的
     * @param tableName
     * @param columnName
     * @return
     */
    private DataType.Name getDataType(String tableName, String columnName) {
        if(getFilterInternalData(DATA_TYPE_NAME) == null) {
            if(StringUtils.isEmpty(tableName) || StringUtils.isEmpty(columnName)) {
                tableName = getFilterInternalData(TABLE_NAME).toString();
                columnName = getFilterInternalData(COLUMN_NAME).toString();
            }

            // 判断采用的是那种增量模式,判断check_column的类型
            StringBuilder metaStr = new StringBuilder("select ");
            metaStr.append(escapeColName(columnName))
                    .append(" from ")
                    .append(escapeTableName(tableName))
                    .append(" limit 1");
            ColumnDefinitions columnDefinitions = session.execute(metaStr.toString()).getColumnDefinitions();
            DataType dataType = columnDefinitions.asList().get(0).getType();

            DataType.Name type = dataType.getName();
            setFilterInternalData(DATA_TYPE_NAME, type);
        }

        return (DataType.Name)getFilterInternalData(DATA_TYPE_NAME);
    }

    @Override protected String escapeColName(String colName) {
        return "\"" + colName + "\"";
    }

    @Override protected String escapeTableName(String tableName) {
        return escapeColName(tableName);
    }

    @Override
    public void close() {
        if(session != null && !session.isClosed()) {
            session.close();
        }
    }

    @Override
    protected String getMaxValueByColumn(String[] tableName, String columnName,String lastValue,int limit) {
        return null;
    }

    @Override
    protected DataBaseType getDbType() {
        return DataBaseType.CASSANDRA;
    }
}
