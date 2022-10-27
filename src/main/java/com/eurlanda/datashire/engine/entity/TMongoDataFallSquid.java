package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.util.MongoSquidUtil;
import org.apache.spark.CustomJavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashSet;
import java.util.Set;

/**
 * 落地 Squid
 * Created by Juntao.Zhang on 2014/4/14.
 */
public class TMongoDataFallSquid extends TSquid {
    private static final long serialVersionUID = 1L;

    private TNoSQLDataSource dataSource;
    private Set<TColumn> columns = new HashSet<>(); // value: keyId = Column.id
    private TSquid previousSquid;
    //是否清空表格
    private boolean truncateExistingData = false;

    public TMongoDataFallSquid() {
        this.setType(TSquidType.MONGO_DATA_FALL_SQUID);
    }

    @Override
    public Boolean run(JavaSparkContext jsc) throws EngineException {
        if (previousSquid.getOutRDD() == null) {
            previousSquid.runSquid(jsc);
        }
        if (previousSquid.getOutRDD() == null) {
            return false;
        }
        MongoSquidUtil.saveToMongo(dataSource, columns, this.getPreviousSquid().getOutRDD().rdd(), (CustomJavaSparkContext) jsc, truncateExistingData);
        return true;
    }

    public TNoSQLDataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(TNoSQLDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Set<TColumn> getColumnSet() {
        return columns;
    }

    public void setColumnSet(Set<TColumn> columnMap) {
        this.columns = columnMap;
    }

    public TSquid getPreviousSquid() {
        return previousSquid;
    }

    public void setPreviousSquid(TSquid previousSquid) {
        this.previousSquid = previousSquid;
    }

    public boolean isTruncateExistingData() {
        return truncateExistingData;
    }

    public void setTruncateExistingData(boolean truncateExistingData) {
        this.truncateExistingData = truncateExistingData;
    }
}
