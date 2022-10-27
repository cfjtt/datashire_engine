package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.engine.dao.SquidFlowDao;
import com.eurlanda.datashire.engine.entity.clean.Cleaner;
import com.eurlanda.datashire.engine.entity.clean.UpdateExtractLastValueInfo;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid;
import com.eurlanda.datashire.engine.spark.util.EngineUtil;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.DSUtil;
import com.eurlanda.datashire.engine.util.ServerRpcUtil;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;

/**
 * Created by zhudebin on 2017/3/22.
 */
public class TCassandraSquid extends TSquid implements IExtractSquid {

    private static Log log = LogFactory.getLog(TCassandraSquid.class);

    {
        this.setType(TSquidType.CASSANDRA_EXTRACT_SQUID);
    }
    // ip用英文逗号[,]间隔
    private String host;
    private String port;
    private String cluster;
    private String keyspace;
    // 0 No, 1:用户名/密码
    private int validate_type;
    private String username;
    private String password;
    // 数据库名.表名
    private String tableName;
    // 抽取的列
    // 导出来的列,必须包含值的字段: data_type{导出的数据类型},name{导出列的名字},refName{导入列的名字}
    // id{导出的ID},(isSourceColumn{是否是从上游reference导入})
    private List<TColumn> columns;

    // 表的别名
    private String alias;

    // 过滤条件
    private String filter;
    // true 存在需要抽取的增量数据; false 不存在需要抽取的增量数据
    private Boolean existIncrementalData = true;

    private UpdateExtractLastValueInfo updateExtractLastValueInfo;

    // connection squid id
    private Integer connectionSquidId;

    @Override protected Object run(JavaSparkContext jsc) throws EngineException {
        // 发送connectionSquid运行成功
        ServerRpcUtil.sendConnectionSquidSuccess(isDebug(),this, connectionSquidId);
        List<Map<Integer, TStructField>> id2Columns;
        Map<Integer, TStructField> id2column = new HashMap<>();     // cassandra抽取squid输出的schema
        Map<Integer, TStructField> extractId2column = new HashMap<>(); // cassandra抽取时的schema
        Map<Integer,TStructField> specialTypecolumn = new HashMap<>();
        for(TColumn c : columns) {
            id2column.put(c.getId(), new TStructField(c.getName(), c.getData_type(), c.isNullable(), c.getPrecision(), c.getScale()));
            extractId2column.put(c.getId(), new TStructField(c.getRefName(), c.getData_type(), c.isNullable(), c.getPrecision(), c.getScale()));
            if(c.getDbBaseDatatype() == DbBaseDatatype.MAP
                    || c.getDbBaseDatatype() == DbBaseDatatype.SET
                    || c.getDbBaseDatatype() == DbBaseDatatype.LIST){
                specialTypecolumn.put(c.getId(),new TStructField(c.getRefName(), c.getData_type(), c.isNullable(), c.getPrecision(), c.getScale()));
            }
        }
        id2Columns = Arrays.asList(id2column);

        SparkSession session = getJobContext().getSparkSession();

        Dataset<Row> ds = null;
        if(existIncrementalData) {

            Map<String, String> options = new HashMap<>();
            options.put("table", tableName);
            options.put("keyspace", keyspace);
            options.put("cluster", cluster);
            options.put("cassandra.validate_type", validate_type + "");
            if(validate_type == 1) {
                options.put("engine.spark.cassandra.username", username);
                options.put("engine.spark.cassandra.password", password);
            } else {
                options.put("engine.spark.cassandra.username", "__no__");
                options.put("engine.spark.cassandra.password", "__no__");
            }
            options.put("engine.spark.cassandra.host", host);
            options.put("engine.spark.cassandra.port", port);
            if (StringUtils.isNotEmpty(filter)) {
                log.info("--cassandra抽取的filter表达式为:" + filter);
                options.put("engine.spark.cassandra.extract.filter", filter);
            }

            ds = session.read()
                    .format("org.apache.spark.sql.cassandra")
                    .options(options)
                    //.schema(TJoinSquid.genStructType(extractId2column))
                    .load()
//                    .coalesce(1)
                    .alias(alias);


            //        if(StringUtils.isNotEmpty(filter)) {
            //            ds = ds.filter(filter);
            //        }
        } else {
            ds = EngineUtil.emptyDataFrame(session, TJoinSquid.genStructType(extractId2column));
//            ds = ds.selectExpr(genSelectColumns(columns));
        }
        ds = ds.selectExpr(genSelectColumns(columns));   // 重命名列和生成抽取时间

        if(DSUtil.isNeedCoalesce(this.getName())) {
            ds = DSUtil.coalesceDataSet(this.getName(), ds);
        }

        // 判断是否需要缓存,根据squid的名字是否有后缀  _c
        boolean isCached = DSUtil.isNeedCache(this.getName());
        if(isCached) {
            ds = DSUtil.cacheDataset(this.getName(), ds);

            log.info("缓存数据---" + this.getName());
            final Dataset<Row> cachedDS = ds;
            this.getCurrentFlow().addCleaner(new Cleaner() {
                @Override public void doSuccess() {
                    cachedDS.unpersist(false);
                }
            });
        }
        outDataFrame = ds;

        outRDD = TJoinSquid.dataFrameToRDDByTDataType(ds, id2Columns).toJavaRDD();

        if(specialTypecolumn.size()>0){
            outRDD = TJoinSquid.rddSpecialTypeToString(outRDD,specialTypecolumn);
            outDataFrame = TJoinSquid.rddToDataFrame(session,this.getName(),outRDD.rdd(),extractId2column);
            //outDataFrame = TJoinSquid.dataFrameSpecialTypeToString(session,outDataFrame,specialTypecolumn,extractId2column,this.getName());
        }
        return null;
    }

    private String[] genSelectColumns(List<TColumn> columns) {
        String[] cExprs = new String[columns.size()];
        for(int i=0; i<columns.size(); i++) {
            TColumn c = columns.get(i);
            boolean isSourceColumn = c.isSourceColumn();
            if(!isSourceColumn ) {
                if(c.getName().equals(ConstantsUtil.CN_EXTRACTION_DATE)) {
                    // 系统列,抽取时间
                    cExprs[i]  = "now() as " + c.getName();
                } else {
                    throw new RuntimeException("不存在系统列" + c.getName());
                }
            } else {
                cExprs[i] = c.getRefName() + " as " + c.getName();
            }
        }
        return cExprs;
    }

    @Override protected void clean() {
        super.clean();
        this.getCurrentFlow().addCleaner(new Cleaner() {
            public void doSuccess() {
                updateLastValue();
            }
        });
    }

    private void updateLastValue() {
        if(updateExtractLastValueInfo != null && existIncrementalData) {
            // 更新数据库中lastvalue的值
            SquidFlowDao squidFlowDao = ConstantUtil.getSquidFlowDao();
            squidFlowDao.updateExtractLastValue(updateExtractLastValueInfo.getSquidId(),
                    updateExtractLastValueInfo.getNewLastValue());
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<TColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<TColumn> columns) {
        this.columns = columns;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public int getValidate_type() {
        return validate_type;
    }

    public void setValidate_type(int validate_type) {
        this.validate_type = validate_type;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getConnectionSquidId() {
        return connectionSquidId;
    }

    public void setConnectionSquidId(Integer connectionSquidId) {
        this.connectionSquidId = connectionSquidId;
    }

    public UpdateExtractLastValueInfo getUpdateExtractLastValueInfo() {
        return updateExtractLastValueInfo;
    }

    public void setUpdateExtractLastValueInfo(
            UpdateExtractLastValueInfo updateExtractLastValueInfo) {
        this.updateExtractLastValueInfo = updateExtractLastValueInfo;
    }

    public Boolean getExistIncrementalData() {
        return existIncrementalData;
    }

    public void setExistIncrementalData(Boolean existIncrementalData) {
        this.existIncrementalData = existIncrementalData;
    }
}
