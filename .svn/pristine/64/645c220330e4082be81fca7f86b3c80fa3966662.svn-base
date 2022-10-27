package com.eurlanda.datashire.engine.spark.db;

import com.eurlanda.datashire.engine.spark.SplitJdbcPartition;
import com.eurlanda.datashire.enumeration.DataBaseType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhudebin on 15-7-23.
 */
public class BigDecimalSplitter extends DBSplitter {

    private static final Log LOG = LogFactory.getLog(BigDecimalSplitter.class);

    public BigDecimalSplitter(int numSplitsParam, List<Object> params, DataBaseType dbType) {
        super(numSplitsParam, params, dbType);
    }

    @Override
    public List<SplitJdbcPartition> split(ResultSet results, String colName) throws SQLException {
        int idx = 0;
        BigDecimal minVal = results.getBigDecimal(1);
        BigDecimal maxVal = results.getBigDecimal(2);

        String lowClausePrefix = colName + " >= ";
        String highClausePrefix = colName + " < ";

        BigDecimal numSplits = new BigDecimal(this.getNumSplits());

        if (minVal == null && maxVal == null) {
            // Range is null to null. Return a null split accordingly.
            List<SplitJdbcPartition> splits = new ArrayList<>();
            splits.add(new SplitJdbcPartition(idx++,
                    colName + " IS NULL", colName + " IS NULL", params));
            return splits;
        }

        if (minVal == null || maxVal == null) {
            // Don't know what is a reasonable min/max value for interpolation. Fail.
            LOG.error("Cannot find a range for NUMERIC or DECIMAL "
                    + "fields with one end NULL.");
            return null;
        }

        // Get all the split points together.
        List<BigDecimal> splitPoints = split(numSplits, minVal, maxVal);
        List<SplitJdbcPartition> splits = new ArrayList<>();

        // Turn the split points into a set of intervals.
        BigDecimal start = splitPoints.get(0);
        for (int i = 1; i < splitPoints.size(); i++) {
            BigDecimal end = splitPoints.get(i);

            if (i == splitPoints.size() - 1) {
                // This is the last one; use a closed interval.
                splits.add(new SplitJdbcPartition(idx++,
                        lowClausePrefix + start.toString(),
                        colName + " <= " + end.toString(), params));
            } else {
                // Normal open-interval case.
                splits.add(new SplitJdbcPartition(idx++,
                        lowClausePrefix + start.toString(),
                        highClausePrefix + end.toString(), params));
            }

            start = end;
        }
        printSplit(splits);
        return splits;
    }

    private static final BigDecimal MIN_INCREMENT =
            new BigDecimal(10000 * Double.MIN_VALUE);

    /**
     * Divide numerator by denominator. If impossible in exact mode, use rounding.
     */
    protected BigDecimal tryDivide(BigDecimal numerator, BigDecimal denominator) {
        try {
            return numerator.divide(denominator);
        } catch (ArithmeticException ae) {
            return numerator.divide(denominator, BigDecimal.ROUND_UP);
        }
    }

    /**
     * Returns a list of BigDecimals one element longer than the list of input
     * splits.  This represents the boundaries between input splits.  All splits
     * are open on the top end, except the last one.
     *
     * So the list [0, 5, 8, 12, 18] would represent splits capturing the
     * intervals:
     *
     * [0, 5)
     * [5, 8)
     * [8, 12)
     * [12, 18] note the closed interval for the last split.
     */
    protected List<BigDecimal> split(BigDecimal numSplits, BigDecimal minVal,
                                     BigDecimal maxVal) throws SQLException {

        List<BigDecimal> splits = new ArrayList<BigDecimal>();

        // Use numSplits as a hint. May need an extra task if the size doesn't
        // divide cleanly.

        BigDecimal splitSize = tryDivide(maxVal.subtract(minVal), (numSplits));
        if (splitSize.compareTo(MIN_INCREMENT) < 0) {
            splitSize = MIN_INCREMENT;
            LOG.warn("Set BigDecimal splitSize to MIN_INCREMENT");
        }

        BigDecimal curVal = minVal;

        while (curVal.compareTo(maxVal) <= 0) {
            splits.add(curVal);
            curVal = curVal.add(splitSize);
        }

        if (splits.get(splits.size() - 1).compareTo(maxVal) != 0
                || splits.size() == 1) {
            // We didn't end on the maxVal. Add that to the end of the list.
            splits.add(maxVal);
        }

        return splits;
    }
}
