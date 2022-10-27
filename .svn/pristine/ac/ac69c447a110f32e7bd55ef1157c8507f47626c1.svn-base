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
public class BooleanSplitter extends DBSplitter {

    private static final Log LOG = LogFactory.getLog(BooleanSplitter.class);

    public BooleanSplitter(int numSplits, List<Object> params, DataBaseType dbType) {
        super(numSplits, params, dbType);
    }

    @Override
    public List<SplitJdbcPartition> split(ResultSet results, String colName) throws SQLException {
        int idx = 0;

        List<SplitJdbcPartition> splits = new ArrayList<>();

        if (results.getString(1) == null && results.getString(2) == null) {
            // Range is null to null. Return a null split accordingly.
            splits.add(new SplitJdbcPartition(idx++,
                    colName + " IS NULL", colName + " IS NULL", params));
            return splits;
        }

        boolean minVal = results.getBoolean(1);
        boolean maxVal = results.getBoolean(2);

        // Use one or two splits.
        if (!minVal) {
            splits.add(new SplitJdbcPartition(idx++,
                    colName + " = FALSE", null, params));
        }

        if (maxVal) {
            splits.add(new SplitJdbcPartition(idx++,
                    colName + " = TRUE", null, params));
        }

        if (results.getString(1) == null || results.getString(2) == null) {
            // Include a null value.
            splits.add(new SplitJdbcPartition(idx++,
                    colName + " IS NULL", null, params));
        }
        printSplit(splits);
        return splits;
    }

}
