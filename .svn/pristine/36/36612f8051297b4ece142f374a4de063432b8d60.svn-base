package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.dao.SquidFlowDao;
import com.eurlanda.datashire.engine.entity.clean.Cleaner;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.ServerRpcUtil;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import org.apache.spark.CustomJavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TDatabaseSquid extends TSquid implements IExtractSquid {
    private static final long serialVersionUID = 1L;
    private Integer dataMiningModelVersionColumnId;
    private TDataSource dataSource;
    private Set<TColumn> columnSet = new HashSet<>(); // value: keyId = Column.id
    // topN
    private Integer topN;
    // connection squid id
    private Integer connectionSquidId;
    private TColumn splitCol;
    // 单次最大抽取数量
    private Integer max_extract_numberPerTimes;
    // 是否合表
    private boolean is_union_table;
    /*// 需要合表的表的名字
    private List<String> tableNames;*/
    private int splitNum;

    public TDatabaseSquid() {
        this.setType(TSquidType.DATABASE_SQUID);
    }

    @Override
    public JavaRDD<Map<Integer, DataCell>> run(JavaSparkContext jsc) {
        if (this.outRDD != null) {
            return this.outRDD;
        }
        // 发送connectionSquid运行成功
        ServerRpcUtil.sendConnectionSquidSuccess(isDebug(),this, connectionSquidId);
        // 对过滤条件trim
        dataSource.setFilter(dataSource.getFilter() ==null ? "" :  dataSource.getFilter().trim());
        //添加限制条件
        dataSource.setLimitNum(max_extract_numberPerTimes == null ? 0 : max_extract_numberPerTimes);
        //需要合并的表
        String[] tableArray = dataSource.getTableNames().toArray(new String[0]);
//        outRDD = ((CustomJavaSparkContext) jsc).jdbcRDD(dataSource, columnSet.toArray(new TColumn[columnSet.size()]), topN);
        // 如果沒有指定SPLIT column则使用分页抽取
        if(splitCol == null) {
            //throw new EngineException("沒有指定分片列");
            //判断是否需要合表
            //if(is_union_table){
                if(tableArray!=null && tableArray.length>0){
                    for(String tableName : tableArray){
                        dataSource.setTableName(tableName);
                        JavaRDD<Map<Integer,DataCell>> outRdd = ((CustomJavaSparkContext) jsc).jdbcRDD(dataSource, columnSet.toArray(new TColumn[columnSet.size()]), topN);
                        if(this.outRDD==null){
                            outRDD = outRdd;
                        } else {
                            outRDD = outRDD.union(outRdd);
                        }
                    }
                }
            /*} else {
                outRDD = ((CustomJavaSparkContext) jsc).jdbcRDD(dataSource, columnSet.toArray(new TColumn[columnSet.size()]), topN);
            }*/
        } else {
            if(dataSource.getType() == DataBaseType.HBASE_PHOENIX) {
                outRDD = ((CustomJavaSparkContext) jsc).phoenixJdbcRDD(dataSource, columnSet.toArray(new TColumn[columnSet.size()]), topN);
            } else {
                // 验证分片列类型
                Map<String, String> splitColInfo = ConfigurationUtil.getSplitColumnInfo();
                if(splitNum > 1 && splitColInfo.get(dataSource.getType().name()) != null) {
                    String[] types = splitColInfo.get(dataSource.getType().name()).split(",");
                    DbBaseDatatype splitColType = splitCol.getDbBaseDatatype();
                    for(String type : types) {
                        if(type.equalsIgnoreCase(splitColType.name())) {
                            throw new RuntimeException("数据库" + dataSource.getType().name() + "不能使用" + splitColType.name() + "类型的" + splitCol + "作为分片列,如果没有其他列,请选择分片数为1");
                        }
                    }
                }

                //是循环，还是在里面?
                //if(is_union_table){
                    if(tableArray!=null && tableArray.length>0){
                        for(String tableName : tableArray){
                            dataSource.setTableName(tableName);
                            JavaRDD<Map<Integer,DataCell>> outRdd = ((CustomJavaSparkContext) jsc).splitJdbcRDD(dataSource, splitCol.getName(), splitNum,
                                    columnSet.toArray(new TColumn[columnSet.size()]));
                            if(this.outRDD==null){
                                outRDD = outRdd;
                            } else {
                                outRDD = outRDD.union(outRdd);
                            }
                        }
                    }else { // 不加else会导致 dataMining的outRDD==null
                        outRDD =
                                ((CustomJavaSparkContext) jsc).splitJdbcRDD(dataSource, splitCol.getName(), splitNum,
                                        columnSet.toArray(new TColumn[columnSet.size()]));
                    }
                /*} else {
                    outRDD =
                            ((CustomJavaSparkContext) jsc).splitJdbcRDD(dataSource, splitCol.getName(), splitNum,
                                    columnSet.toArray(new TColumn[columnSet.size()]));
                }*/
            }
        }

       // return outRDD;
        if (dataMiningModelVersionColumnId == null){
            return outRDD;
        } else {
         //   return getDataMiningMaxVersionModelRDD(outRDD); // 在data mining squid上设置查看器时，只查看最新的版本
            outRDD = getDataMiningMaxVersionModelRDD(outRDD); 
            return outRDD; 
        }
    }

    public TDataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(TDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Set<TColumn> getColumnSet() {
		return columnSet;
	}

	public void setColumnSet(Set<TColumn> columnSet) {
		this.columnSet = columnSet;
	}

	public void putColumn(TColumn column) {
		columnSet.add(column);
    }

    public void setTopN(Integer topN) {
        this.topN = topN;
    }

    public Integer getConnectionSquidId() {
        return connectionSquidId;
    }

    public void setConnectionSquidId(Integer connectionSquidId) {
        this.connectionSquidId = connectionSquidId;
    }

    public TColumn getSplitCol() {
        return splitCol;
    }

    public void setSplitCol(TColumn splitCol) {
        this.splitCol = splitCol;
    }

    public int getSplitNum() {
        return splitNum;
    }

    public void setSplitNum(int splitNum) {
        this.splitNum = splitNum;
    }


    public void  setDataMiningModelVersionColumnId(Integer modelVersionColumnId){
        this.dataMiningModelVersionColumnId = modelVersionColumnId;
    }

    public Integer getMax_extract_numberPerTimes() {
        return max_extract_numberPerTimes;
    }

    public void setMax_extract_numberPerTimes(Integer max_extract_numberPerTimes) {
        this.max_extract_numberPerTimes = max_extract_numberPerTimes;
    }

    public boolean isIs_union_table() {
        return is_union_table;
    }

    public void setIs_union_table(boolean is_union_table) {
        this.is_union_table = is_union_table;
    }

    /**
     * data mining squid 上设置查看器时，只抽取最新版本
     * @return
     */
    private JavaRDD<Map<Integer, DataCell>> getDataMiningMaxVersionModelRDD(JavaRDD<Map<Integer, DataCell>> outRDD) {
        if(outRDD == null){
            throw new RuntimeException("getDataMiningMaxVersionModelRDD的入参是null");
        }
        if(outRDD.isEmpty()){
            return outRDD;
        }
        // 记录最大的 version， 关联规则的相同version有多行
       final int modelVersionColumnIdtmp = this.dataMiningModelVersionColumnId;
       JavaRDD<Short> versionRdd = outRDD.map(new Function<Map<Integer, DataCell>, Short>() {
            @Override
            public Short call(Map<Integer, DataCell> v1) throws Exception {
                return Short.valueOf(v1.get(modelVersionColumnIdtmp).getData().toString()); // 不能强转，否则会报java.lang.Integer cannot be cast to java.lang.Short
            }
        }).distinct();
        final Integer maxVersion = Collections.max(versionRdd.collect()).intValue();
        return outRDD.filter(new Function<Map<Integer, DataCell>, Boolean>() {
            @Override
            public Boolean call(Map<Integer, DataCell> v1) throws Exception {
                return v1.get(modelVersionColumnIdtmp).getData().toString().equals(maxVersion.toString());
            }
        });
    }

    @Override
    protected void clean() {
        super.clean();
        this.getCurrentFlow().addCleaner(new Cleaner() {
            public void doSuccess() {
                if(dataSource.getUpdateLastValueInfo() != null && dataSource.getExistIncrementalData()) {
                    // 更新数据库中lastvalue的值
                    SquidFlowDao squidFlowDao = ConstantUtil.getSquidFlowDao();
                    squidFlowDao.updateExtractLastValue(dataSource.getUpdateLastValueInfo().getSquidId(),
                            dataSource.getUpdateLastValueInfo().getNewLastValue());
                }
            }
        });
    }
}
