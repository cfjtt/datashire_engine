package com.eurlanda.datashire.engine.spark.statistics

import java.util

import com.eurlanda.datashire.engine.spark.statistics.StatisticsAlgorithmName.StatisticsAlgorithmName
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql.{DataFrame, Row, RowFactory}
import org.apache.spark.sql.types.{DecimalType, DoubleType, StructField, StructType}

/**
  * Created by Administrator on 2017-05-09.
  * 输入一个数值数组，输出一个值
  * 标准差，峰度，百分位数
  */
class Input1ColumnOutput1Value(algorithmName:StatisticsAlgorithmName,params: util.HashMap[String, Object]) extends Serializable {

  private val log: Log = LogFactory.getLog(classOf[Input1ColumnOutput1Value])



  def run(dataFrame:DataFrame): DataFrame = {

    try {
      if (dataFrame == null) {
        throw new IllegalArgumentException(algorithmName + "统计计算的输入数据是null")
      }
      val colmunes = dataFrame.columns
      if (colmunes.size != 1) {
        throw new IllegalArgumentException("输入数据不是1列")
      }

      val dataFrameProcessNa = dataFrame.na.drop()
      if(dataFrameProcessNa == null || dataFrameProcessNa.rdd.isEmpty()){
        throw new IllegalArgumentException(algorithmName + "数据是null或空字符串,可能原因：decimal的scale设置过大")
      }
      if (algorithmName.equals(StatisticsAlgorithmName.STDDEV)) {
        //值是否全相等
        val max = dataFrameProcessNa.agg(Map(colmunes.apply(0) -> "max")).take(1).apply(0).getDecimal(0)
        val min = dataFrameProcessNa.agg(Map(colmunes.apply(0) -> "min")).take(1).apply(0).getDecimal(0)
        if (max.equals(min)) {
          val rowList = new java.util.ArrayList[Row]()
          rowList.add(RowFactory.create(Double.box(0)))
          val fields = new Array[StructField](1)
          fields.update(0, new StructField("Stddev", DoubleType))
          val structType = StructType(fields)
          return dataFrameProcessNa.sparkSession.createDataFrame(rowList, structType)
        }
        val resdf = dataFrameProcessNa.agg(Map(colmunes.apply(0) -> "stddev")).toDF("Stddev")
        if (resdf.take(1).apply(0).getDouble(0).equals(Double.NaN)) {
          throw new IllegalArgumentException("标准差的入参不能是null或只有一行")
        }
        return resdf
      } else if (algorithmName.equals(StatisticsAlgorithmName.KURTOSIS)) {
        val resdf = dataFrameProcessNa.selectExpr("kurtosis(" + colmunes.apply(0) + ")").toDF("Kurtosis")
        return resdf
      } else if (algorithmName.equals(StatisticsAlgorithmName.PERCENTILE)) {
        // 控制参数
        if (params == null) {
          throw new IllegalArgumentException("百分位数的参数不能是null或空字符串")
        }
        val quantile = params.get("Quantile")
        if (quantile == null || quantile.toString.trim.equals("")) {
          throw new IllegalArgumentException("百分位数的参数不能是null或空字符串")
        }
        var quantileValue = 0.0
        try {
          quantileValue = quantile.toString.toDouble
        } catch {
          case _: Exception => throw new IllegalArgumentException("百分位数的参数Quantile不是数字")
        }
        if (quantileValue < 0.0 || quantileValue > 1.0) {
          throw new IllegalArgumentException("百分位数的参数Quantile不在区间[0,1]内")
        }
        val sql = "percentile (" + colmunes.apply(0) + "," + quantileValue + ")"
        dataFrameProcessNa.selectExpr(sql).toDF("Percentil")
      } else {
        throw new IllegalArgumentException("不存在算法" + algorithmName)
      }
    }catch {
      case e:Throwable =>{
        val errorMessage = e.getMessage
        log.error(errorMessage)
        throw new RuntimeException(e)
      }
    }

  }


}
