package com.eurlanda.datashire.engine.spark.statistics

import java.util

import com.eurlanda.datashire.engine.spark.statistics.StatisticsAlgorithmName.StatisticsAlgorithmName
import com.eurlanda.datashire.engine.spark.statistics.timeseries.MovingAverage
import org.apache.spark.sql._

/**
  * Created by Administrator on 2017-05-09.
  *
  * 输入一列值，输出多个值
  * 移动平均
  * */
class Input1ColumnOutput1Column(algorithmName:StatisticsAlgorithmName,dataFrame:DataFrame) extends Serializable {

  val colmunes = dataFrame.columns
  if (colmunes.size != 1) {
    throw new IllegalArgumentException("输入数据不是1列")
  }

  def run(params: util.HashMap[String, Object]): DataFrame = {
    if (algorithmName.equals(StatisticsAlgorithmName.MOVINGAVERAGE)) {
      MovingAverage.run(dataFrame, params)
    } else {
      throw new IllegalArgumentException("不存在算法" + algorithmName)
    }
  }


}
