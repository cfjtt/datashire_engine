/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * Implement DBSplitter over floating-point values.
 */
public class FloatSplitter extends DBSplitter {

  private static final Log LOG = LogFactory.getLog(FloatSplitter.class);

  private static final double MIN_INCREMENT = 10000 * Double.MIN_VALUE;

  public FloatSplitter(int numSplits, List<Object> params, DataBaseType dbType) {
    super(numSplits, params, dbType);
  }

  public List<SplitJdbcPartition> split(ResultSet results,
      String colName) throws SQLException {

    LOG.warn("Generating splits for a floating-point index column. Due to the");
    LOG.warn("imprecise representation of floating-point values in Java, this");
    LOG.warn("may result in an incomplete import.");
    LOG.warn("You are strongly encouraged to choose an integral split column.");

    List<SplitJdbcPartition> splits = new ArrayList<>();
    int idx = 0;

    if (results.getString(1) == null && results.getString(2) == null) {
      // Range is null to null. Return a null split accordingly.
      splits.add(new SplitJdbcPartition(idx++,
          colName + " IS NULL", null, params));
      return splits;
    }

    double minVal = results.getDouble(1);
    double maxVal = results.getDouble(2);

    // Use this as a hint. May need an extra task if the size doesn't
    // divide cleanly.
    double splitSize = (maxVal - minVal) / (double) numSplits;

    if (splitSize < MIN_INCREMENT) {
      splitSize = MIN_INCREMENT;
    } else if(Double.isInfinite(splitSize)) { // 超过了double的最大值
        splitSize = Double.MAX_VALUE / (double) numSplits;
    }

    String lowClausePrefix = colName + " >= ";
    String highClausePrefix = colName + " < ";

    double curLower = minVal;
    double curUpper = curLower + splitSize;

    boolean isFirst = true;
    while (curUpper < maxVal && !Double.isInfinite(curUpper)) {
      if(isFirst) { // 第一个不需要指定从哪开始
          splits.add(new SplitJdbcPartition(idx++,
                  "1=1",
                  highClausePrefix + Double.toString(curUpper), params));
          isFirst = false;
      } else {
          splits.add(new SplitJdbcPartition(idx++,
                  lowClausePrefix + Double.toString(curLower),
                  highClausePrefix + Double.toString(curUpper), params));
      }
      curLower = curUpper;
      curUpper += splitSize;
    }

    // Catch any overage and create the closed interval for the last split.
    if (curLower <= maxVal || splits.size() == 1) {
      splits.add(new SplitJdbcPartition(idx ++,
//          lowClausePrefix + Double.toString(curUpper),
          lowClausePrefix + Double.toString(curLower),
//          colName + " <= " + Double.toString(maxVal), params));
          // 最后一个不需要指定到哪结束
          "1=1", params));
    }

    if (results.getString(1) == null || results.getString(2) == null) {
      // At least one extrema is null; add a null split.
      splits.add(new SplitJdbcPartition(idx ++,
          colName + " IS NULL", null, params));
    }
    printSplit(splits);
    return splits;
  }
}
