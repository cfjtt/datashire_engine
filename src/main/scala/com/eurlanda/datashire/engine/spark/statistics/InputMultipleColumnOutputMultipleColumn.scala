package com.eurlanda.datashire.engine.spark.statistics

import java.util

import com.eurlanda.datashire.engine.spark.statistics.anova.OneWayAnova
import com.eurlanda.datashire.engine.spark.statistics.StatisticsAlgorithmName.StatisticsAlgorithmName
import org.apache.spark.sql._



/**
  * Created by Administrator on 2017-05-09.
  * 输入多列，输出多个值
  * 单因素方差分析
  */
class InputMultipleColumnOutputMultipleColumn (algorithmName:StatisticsAlgorithmName, dataFrame: DataFrame) extends Serializable {

  def run(params: util.HashMap[String, Object]): DataFrame = {

    if (algorithmName.equals(StatisticsAlgorithmName.ONEWAYANOVA)) {
      // 单因素方差分析
      OneWayAnova.run(dataFrame, params)
    } else {
      throw new IllegalArgumentException("不存在算法" + algorithmName)
    }
  }

}
