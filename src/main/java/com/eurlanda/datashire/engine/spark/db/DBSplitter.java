package com.eurlanda.datashire.engine.spark.db;

import com.eurlanda.datashire.engine.spark.SplitJdbcPartition;
import com.eurlanda.datashire.enumeration.DataBaseType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhudebin on 15-7-23.
 */
public abstract class DBSplitter {

    private static final Log LOG = LogFactory.getLog(DBSplitter.class);

    protected int numSplits;
    protected List<Object> params;
    protected DataBaseType dbType;

    public DBSplitter(int numSplits, List<Object> params, DataBaseType dbType) {
        this.numSplits = numSplits;
        this.params = params;
        this.dbType = dbType;
    }

    /**
     * Given a ResultSet containing one record (and already advanced to that
     * record) with two columns (a low value, and a high value, both of the same
     * type), determine a set of splits that span the given values.
     */
    public List<SplitJdbcPartition> splitPartitions(ResultSet results, String colName)
            throws SQLException {
//        if(this.numSplits == 1) {
//            return splitOnePartition();
//        } else {
        // 分区数已经判断过了
        return split(results, colName);
//        }
    }

    /**
     * Given a ResultSet containing one record (and already advanced to that
     * record) with two columns (a low value, and a high value, both of the same
     * type), determine a set of splits that span the given values.
     */
    protected abstract List<SplitJdbcPartition> split(ResultSet results, String colName)
            throws SQLException;

    public static List<SplitJdbcPartition> splitOnePartition(List<Object> params) {
        List<SplitJdbcPartition> splits = new ArrayList<>();
        splits.add(new SplitJdbcPartition(0, "1=1", "", params));
        return splits;
    }

    protected void printSplit(List<SplitJdbcPartition> partitions) {
        if(LOG.isDebugEnabled()) {
            LOG.info("-----------splitter--------" + getClass().getSimpleName());
            LOG.info("分割总数为：" + partitions.size());
            for (SplitJdbcPartition part : partitions) {
                LOG.info(jdbcPartitionInfo(part));
            }
            LOG.info("--------------- split info ----------------");
        }
    }

    private String jdbcPartitionInfo(SplitJdbcPartition part) {
        StringBuilder sb = new StringBuilder();
        sb.append("\t [")
                .append(part.lower())
                .append(",")
                .append(part.upper()).append("],params:[");
        for(Object obj : part.params()) {
            sb.append(obj).append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    protected SplitJdbcPartition genPartitionByParams(int idx, String colName, DataBaseType dbType, Object startDate,
                                            Object endDate, List<Object> params, boolean isLast, boolean isFirst) {
        String lowClausePrefix = colName + " >= ";
        String highClausePrefix = colName + " < ";

        List<Object> oparams = null;
        oparams = new ArrayList<>(params);
        oparams.add(startDate);
        oparams.add(endDate);

        if(isLast) {
            return new SplitJdbcPartition(idx, lowClausePrefix + "?",
                    "( 1=1 or " + colName + " <= " + "?" + ")", oparams);
        } else if(isFirst) {
            return new SplitJdbcPartition(idx,
                    "( 1=1 or " + lowClausePrefix + "?" + ")",
                    highClausePrefix + "?", oparams);
        } else {
            return new SplitJdbcPartition(idx,
                    lowClausePrefix + "?",
                    highClausePrefix + "?", oparams);
        }

    }
    protected SplitJdbcPartition genPartitionByStr(int idx, String colName, DataBaseType dbType, String start,
                                            String end, List<Object> params, boolean isLast, boolean isFirst) {
        String lowClausePrefix = colName + " >= ";
        String highClausePrefix = colName + " < ";

        if(isLast) {
            return new SplitJdbcPartition(idx,
                    lowClausePrefix + start,
                    "(1=1 or " + colName + " <= " + end + ")", params);
        } else if(isFirst) {
            return new SplitJdbcPartition(idx,
                    "(1=1 or " + lowClausePrefix + start + ")",
//                    lowClausePrefix + start,
                    highClausePrefix + end, params);
        } else {
            return new SplitJdbcPartition(idx,
                    lowClausePrefix + start,
                    highClausePrefix + end, params);
        }
    }

    protected SplitJdbcPartition genPartitionByText(int idx, String colName, DataBaseType dbType, String start,
                                            String end, List<Object> params, boolean isLast) {
        String lowClausePrefix = colName + " >= ";
        String highClausePrefix = colName + " < ";

        if(isLast) {
            return new SplitJdbcPartition(idx,
                    lowClausePrefix +"'"+start+"'",
                    colName + " <= " +"'"+ end+"'", params);
        } else {
            return new SplitJdbcPartition(idx,
                    lowClausePrefix + "'"+start+"'",
                    highClausePrefix +"'"+ end+"'", params);
        }

    }

    public int getNumSplits() {
        return numSplits;
    }

    public void setNumSplits(int numSplits) {
        this.numSplits = numSplits;
    }

    public List<Object> getParams() {
        return params;
    }

    public void setParams(List<Object> params) {
        this.params = params;
    }
}
