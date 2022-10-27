package com.eurlanda.datashire.engine.spark.db;

import com.eurlanda.datashire.engine.spark.SplitJdbcPartition;
import com.eurlanda.datashire.enumeration.DataBaseType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by zhudebin on 15-7-23.
 */
public class DateSplitter extends IntegerSplitter {

    private static final Log LOG = LogFactory.getLog(DateSplitter.class);

    public DateSplitter(int numSplits, List<Object> params, DataBaseType dbType) {
        super(numSplits, params, dbType);
    }

    @Override
    public List<SplitJdbcPartition> split(ResultSet results, String colName) throws SQLException {
        int idx = 0;

        long minVal;
        long maxVal;

        int sqlDataType = results.getMetaData().getColumnType(1);
        minVal = resultSetColToLong(results, 1, sqlDataType);
        // +1000 解决timetostring 毫秒丢失 后面是L不是1
        maxVal = resultSetColToLong(results, 2, sqlDataType) + 1000l;

        String lowClausePrefix = colName + " >= ";
        String highClausePrefix = colName + " < ";

        if (numSplits < 1) {
            numSplits = 1;
        }

        if (minVal == Long.MIN_VALUE && maxVal-1000l == Long.MIN_VALUE) {
            // The range of acceptable dates is NULL to NULL. Just create a single
            // split.
            List<SplitJdbcPartition> splits = new ArrayList<>();
              //因为null值会在前面做整体的加入，这里如果这一列全为null不必要加入了
//            splits.add(new SplitJdbcPartition(idx++,
//                    colName + " IS NULL", null, params));
            splits.add(new SplitJdbcPartition(idx++,
                    colName + " IS NULL", null, params));
            return splits;
        }

        // Gather the split point integers
        List<Long> splitPoints = split(numSplits, minVal, maxVal);
        List<SplitJdbcPartition> splits = new ArrayList<>();

        // Turn the split points into a set of intervals.
        Date startDate = longToDate(splitPoints.get(0), sqlDataType);
        if (sqlDataType == Types.TIMESTAMP) {
            // The lower bound's nanos value needs to match the actual lower-bound
            // nanos.
            try {
                ((java.sql.Timestamp) startDate).setNanos(
                        results.getTimestamp(1).getNanos());
            } catch (NullPointerException npe) {
                // If the lower bound was NULL, we'll get an NPE; just ignore it and
                // don't set nanos.
            }
        }

        boolean isLast = false;
        boolean isFirst = true;
        for (int i = 1; i < splitPoints.size(); i++) {
            long end = splitPoints.get(i);
            Date endDate = longToDate(end, sqlDataType);

            if( i== 1) {
                isFirst = true;
            } else {
                isFirst = false;
            }

            if(i == splitPoints.size() - 1) {
                isLast = true;
            } else {
                isLast = false;
            }
            if (sqlDataType == Types.TIMESTAMP) {
                // The upper bound's nanos value needs to match the actual
                // upper-bound nanos.
                try {
                    ((java.sql.Timestamp) endDate).setNanos(
                            results.getTimestamp(2).getNanos());
                } catch (NullPointerException npe) {
                    // If the upper bound was NULL, we'll get an NPE; just ignore it
                    // and don't set nanos.
                }
            }
            // This is the last one; use a closed interval.
            splits.add(genPartition(idx++, colName, dbType, startDate, endDate, params, isLast, isFirst));

            startDate = endDate;
        }

        if (minVal == Long.MIN_VALUE || maxVal == Long.MIN_VALUE) {
            // Add an extra split to handle the null case that we saw.
            splits.add(new SplitJdbcPartition(idx++,
                    colName + " IS NULL", null, params));
        }
        printSplit(splits);
        return splits;
    }

    /**
     Retrieve the value from the column in a type-appropriate manner and
     return its timestamp since the epoch. If the column is null, then return
     Long.MIN_VALUE.  This will cause a special split to be generated for the
     NULL case, but may also cause poorly-balanced splits if most of the
     actual dates are positive time since the epoch, etc.
     */
    private long resultSetColToLong(ResultSet rs, int colNum, int sqlDataType)
            throws SQLException {
        try {
            switch (sqlDataType) {
                case Types.DATE:
                    return rs.getDate(colNum).getTime();
                case Types.TIME:
                    return rs.getTime(colNum).getTime();
                case Types.TIMESTAMP:
                    return rs.getTimestamp(colNum).getTime();
                default:
                    throw new SQLException("Not a date-type field");
            }
        } catch (NullPointerException npe) {
            // null column. return minimum long value.
            LOG.warn("Encountered a NULL date in the split column. "
                    + "Splits may be poorly balanced.");
            return Long.MIN_VALUE;
        }
    }

    /**  Parse the long-valued timestamp into the appropriate SQL date type. */
    private Date longToDate(long val, int sqlDataType) {
        switch (sqlDataType) {
            case Types.DATE:
                return new java.sql.Date(val);
            case Types.TIME:
                return new java.sql.Time(val);
            case Types.TIMESTAMP:
                return new java.sql.Timestamp(val);
            default: // Shouldn't ever hit this case.
                return null;
        }
    }

    /**
     * Given a Date 'd', format it as a string for use in a SQL date
     * comparison operation.
     * @param d the date to format.
     * @return the string representing this date in SQL with any appropriate
     * quotation characters, etc.
     */
    protected String dateToString(Date d) {
        return "'" + d.toString() + "'";
    }

    private SplitJdbcPartition genPartition(int idx, String colName, DataBaseType dbType, Date startDate,
                                            Date endDate, List<Object> params, boolean isLast, boolean isFirst) {

        if(dbType == DataBaseType.SQLSERVER) {
            return genPartitionByStr(idx, colName, dbType, dateToString(startDate),
                    dateToString(endDate), params, isLast, isFirst);
        } else {
            return genPartitionByParams(idx, colName, dbType, startDate, endDate, params, isLast, isFirst);
        }
    }
}
