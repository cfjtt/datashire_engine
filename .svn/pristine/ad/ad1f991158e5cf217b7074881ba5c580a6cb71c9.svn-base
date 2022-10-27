package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.util.ServerRpcUtil;
import org.apache.spark.CustomJavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TMongoExtractSquid extends TSquid implements IExtractSquid {
    private static final long serialVersionUID = 1L;
    private TNoSQLDataSource dataSource;
    private Set<TColumn> columnSet = new HashSet<>(); // value: keyId = Column.id
    // topN
    private Integer topN;
    // connection squid id
    private Integer connectionSquidId;

    public TMongoExtractSquid() {
        this.setType(TSquidType.MONGO_EXTRACT_SQUID);
    }

    @Override
    public JavaRDD<Map<Integer, DataCell>> run(JavaSparkContext jsc) {
        if (this.outRDD != null) {
            return this.outRDD;
        }
        // 发送connectionSquid运行成功
        ServerRpcUtil.sendConnectionSquidSuccess(isDebug(),this, connectionSquidId);

        outRDD = ((CustomJavaSparkContext) jsc).mongoRDD(dataSource, columnSet.toArray(new TColumn[columnSet.size()]), topN);
        return outRDD;
    }

    public TNoSQLDataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(TNoSQLDataSource dataSource) {
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
}
