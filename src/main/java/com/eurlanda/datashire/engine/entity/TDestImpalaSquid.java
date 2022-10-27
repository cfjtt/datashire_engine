package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.util.ImpalaSquidUtil;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhudebin on 15-10-12.
 */
public class TDestImpalaSquid extends TSquid {

    private TDataSource dataSource;
    private Set<TColumn> columns;

    {
        this.setType(TSquidType.DEST_IMPALA_SQUID);
    }

    private TSquid previousSquid;


    public TDestImpalaSquid() {
        this.setType(TSquidType.DEST_HDFS_SQUID);
    }

    @Override protected Object run(JavaSparkContext jsc) throws EngineException {

        Map<String, Object> params = new HashMap<>();

        params.put(ImpalaSquidUtil.DATASOURCE(), dataSource);
        params.put(ImpalaSquidUtil.TCOLUMNS(), columns);

        ImpalaSquidUtil.saveToImpala(jsc.sc(), previousSquid.getOutRDD(), params);
        return null;
    }

    public TSquid getPreviousSquid() {
        return previousSquid;
    }

    public void setPreviousSquid(TSquid previousSquid) {
        this.previousSquid = previousSquid;
    }

    public TDataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(TDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Set<TColumn> getColumns() {
        return columns;
    }

    public void setColumns(Set<TColumn> columns) {
        this.columns = columns;
    }
}
