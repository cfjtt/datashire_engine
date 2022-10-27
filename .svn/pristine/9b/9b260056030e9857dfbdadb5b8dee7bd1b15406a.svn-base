package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.common.util.HbaseUtil;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.enumeration.DataBaseType;
import org.apache.spark.CustomJavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashSet;
import java.util.Set;

/**
 * 落地 Squid
 * Created by Juntao.Zhang on 2014/4/14.
 */
public class TDataFallSquid extends TSquid {
    private static final long serialVersionUID = 1L;

    private TDataSource dataSource;
    private Set<TColumn> columns = new HashSet<>(); // value: keyId = Column.id
    private TSquid previousSquid;
    //是否清空表格
    private boolean truncateExistingData = false;

    public TDataFallSquid() {
        this.setType(TSquidType.DATA_FALL_SQUID);
    }

    @Override
    public Boolean run(JavaSparkContext jsc) throws EngineException {
        if (previousSquid.getOutRDD() == null) {
            previousSquid.runSquid(jsc);
        }
        if (previousSquid.getOutRDD() == null) {
            return false;
        }
        // 判断hbase数据库，如果没有主键，则添加一个guid主键列
        if(dataSource.getType() == DataBaseType.HBASE_PHOENIX) {
            int no = 0;
            for(TColumn tc : columns) {
                if(tc.isPrimaryKey()) {
                    no += 1;
                    break;
                }
            }
            if(no == 0) {
                TColumn guidColumn = new TColumn(TDataType.STRING, HbaseUtil.DEFAULT_PK_NAME, -1);
                guidColumn.setSourceColumn(false);
                guidColumn.setGuid(true);
                columns.add(guidColumn);
            }
        }

        ((CustomJavaSparkContext) jsc).fallDataRDD(dataSource, columns, this.getPreviousSquid().getOutRDD(), truncateExistingData);
        return true;
    }

    public TDataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(TDataSource dataSource) {
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
