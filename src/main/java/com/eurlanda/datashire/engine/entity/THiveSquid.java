package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.engine.entity.clean.Cleaner;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid;
import com.eurlanda.datashire.engine.util.DSUtil;
import com.eurlanda.datashire.engine.util.ServerRpcUtil;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 2017/3/22.
 */
public class THiveSquid extends TSquid implements IExtractSquid {

    private static Log log = LogFactory.getLog(THiveSquid.class);

    {
        this.setType(TSquidType.HIVE_EXTRACT_SQUID);
    }

    // 数据库名.表名
    private String tableName;

    // 抽取的列
    // 导出来的列,必须包含值的字段: data_type{导出的数据类型},name{导出列的名字},refName{导入列的名字}
    // id{导出的ID},(isSourceColumn{是否是从上游reference导入})
    private List<TColumn> columns;

    private List<Map<Integer, TStructField>> id2Columns;

    // 表的别名
    private String alias;

    // 过滤条件
    private String filter;

    // connection squid id
    private Integer connectionSquidId;

    @Override protected Object run(JavaSparkContext jsc) throws EngineException {
        // 发送connectionSquid运行成功
        ServerRpcUtil.sendConnectionSquidSuccess(isDebug(),this, connectionSquidId);


        SQLContext sqlCtx = getJobContext().getSqlContext();

        Dataset<Row> ds = sqlCtx.table(tableName).alias(alias);
        if(StringUtils.isNotBlank(filter)) {
            ds = ds.filter(filter);
        }
        ds = ds.selectExpr(genSelectColumns(columns));

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

//        id2Columns = new ArrayList<>();
        Map<Integer, TStructField> id2column = new HashMap<>();
        Map<Integer,TStructField> extractId2column = new HashMap<>();
        Map<Integer,TStructField> specialTypecolumn = new HashMap<>();
        for(TColumn c : columns) {
            id2column.put(c.getId(), new TStructField(c.getName(), c.getData_type(), c.isNullable(), c.getPrecision(), c.getScale()));
            extractId2column.put(c.getId(),new TStructField(c.getRefName(),c.getData_type(),c.isNullable(),c.getPrecision(),c.getScale()));
            if(c.getDbBaseDatatype() == DbBaseDatatype.ARRAY
                    || c.getDbBaseDatatype() == DbBaseDatatype.MAP
                    || c.getDbBaseDatatype() == DbBaseDatatype.UNIONTYPE){
                specialTypecolumn.put(c.getId(),new TStructField(c.getRefName(), c.getData_type(), c.isNullable(), c.getPrecision(), c.getScale()));
            }
        }
        id2Columns = Arrays.asList(id2column);

        outRDD = TJoinSquid.dataFrameToRDDByTDataType(ds, id2Columns).toJavaRDD();
        if(specialTypecolumn.size()>0){
            outRDD = TJoinSquid.rddSpecialTypeToString(outRDD,specialTypecolumn);
            outDataFrame = TJoinSquid.rddToDataFrame(getJobContext().getSparkSession(),this.getName(),outRDD.rdd(),id2column);
            //outDataFrame = TJoinSquid.dataFrameSpecialTypeToString(getJobContext().getSparkSession(),outDataFrame,specialTypecolumn,extractId2column,this.getName());
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
                cExprs[i] = "`" + c.getRefName() + "` as " + c.getName();
            }
        }
        return cExprs;
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

    public Integer getConnectionSquidId() {
        return connectionSquidId;
    }

    public void setConnectionSquidId(Integer connectionSquidId) {
        this.connectionSquidId = connectionSquidId;
    }
}
