package com.eurlanda.datashire.engine.spark.statistics.anova

import java.util

import breeze.stats.distributions.FDistribution
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql.{Row, RowFactory, _}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017-05-09
  * 单因素方差分析，输入至少2列
  */
object OneWayAnova {

  private val log: Log = LogFactory.getLog(OneWayAnova.getClass)

  /**
    * 列数不定，每一列都是一个dataframe
    *
    * @param dataFrame
    * @param params
    * @return
    * SST, dfT,  SSE, dfE , MSE , SSA , dfA, MSA , F , pValue ,FCriterion
    * Double,Long,Double,Long,Double,Double,Long,Double,Double,Double,Double
    */
  def run(dataFrame: DataFrame, params: util.HashMap[String, Object]): DataFrame = {

    if (dataFrame.columns.size < 2) {
      throw new IllegalArgumentException("单因素方差分析的入参至少是2列")
    }
    if (params == null) {
      throw new IllegalArgumentException("单因素方差分析参数α是null或是空字符串")
    }
    val dataFrameProcessNa = dataFrame.na.drop()
    if(dataFrameProcessNa == null || dataFrameProcessNa.rdd.isEmpty()){
      throw new IllegalArgumentException("单因素方差分析数据是null或空字符串,可能原因：decimal的scale设置过大")
    }
    try {
      val alpha = params.get("α")
      if (alpha == null || alpha.toString.trim.equals("")) {
        throw new RuntimeException("单因素方差分析参数α是null或是空字符串")
      }
      var alphaValue = -1.0
      try {
        alphaValue = alpha.toString.toDouble
      } catch {
        case _: Exception => throw new RuntimeException("单因素方差分析参数α不是数字")
      }
      if (alphaValue < 0.0 || alphaValue > 1.0) {
        throw new RuntimeException("单因素方差分析参数α不在区间[0,1]")
      }
      require(alphaValue >= 0.0 && alphaValue <= 1.0, "单因素方差分析的参数水平α不在区间[0,1]内")

      val countsEachColumn = new ArrayBuffer[Long]() // 每列的元素个数
      val avgEachColumn = new ArrayBuffer[BigDecimal]() // 每列的平均值
      var totalsum = BigDecimal.valueOf(0.0)
      if(dataFrame == null || dataFrame.rdd.isEmpty()){
        throw new RuntimeException("没有数据或数据是null")
      }
      val columnName = dataFrame.columns
      if (dataFrame.count() <= 1) {
        throw new IllegalArgumentException("单因素方差分析数据不能只有1行")
      }

      for (colnm <- columnName) {
        val dataFrameProcessNa = dataFrame.select(colnm).na.drop()

        avgEachColumn += dataFrameProcessNa.agg(Map(colnm -> "avg")).head(1).apply(0).getDecimal(0)
        countsEachColumn += dataFrameProcessNa.agg(Map(colnm -> "count")).head(1).apply(0).getLong(0)
        totalsum += dataFrameProcessNa.agg(Map(colnm -> "sum")).head(1).apply(0).getDecimal(0)
      }

      var SSE = BigDecimal.valueOf(0.0) // 组内误差
      for (i <- 0 until (columnName.size)) {

        // 每列的平均值
        val colnme = columnName.apply(i)
        val sumtmp = dataFrame.select(colnme).na.drop().rdd.map(x => {
          // val colsubavg = x.getDecimal(0) - avgEachColumn.apply(i)
          val colsubavg = x.getDecimal(0).subtract(avgEachColumn.apply(i).bigDecimal)
          colsubavg.pow(2)
        }).reduce((a, b) => a.add(b))
        SSE = SSE.bigDecimal.add(sumtmp)
      }

      //SSA,组间 列平均值- 总平均值
      var SSA = BigDecimal.valueOf(0.0) // 组间误差
      val totalavg = totalsum / countsEachColumn.sum
      for (i <- 0.until(avgEachColumn.size)) {
        val avgsuntotalsvg = avgEachColumn.apply(i) - totalavg
        SSA += countsEachColumn.apply(i) * avgsuntotalsvg * avgsuntotalsvg
      }

      if (SSE.precision > DecimalType.MAX_PRECISION) {
        throw new IllegalArgumentException("SSE " + SSE + "超过最大长度" + DecimalType.MAX_PRECISION)
      }
      if (SSE.scale > DecimalType.MAX_SCALE) {
        throw new IllegalArgumentException("SSE " + SSE + "小数位数超过最大长度" + DecimalType.MAX_SCALE)
      }
      if (SSA.precision > DecimalType.MAX_PRECISION) {
        throw new IllegalArgumentException("SSA " + SSA + "超过最大长度" + DecimalType.MAX_PRECISION)
      }
      if (SSA.scale > DecimalType.MAX_SCALE) {
        throw new IllegalArgumentException("SSA " + SSA + "小数位数超过最大长度" + DecimalType.MAX_SCALE)
      }
      val SST = SSA + SSE
      if (SST.precision > DecimalType.MAX_PRECISION) {
        throw new IllegalArgumentException("SST " + SST + "超过最大长度" + DecimalType.MAX_PRECISION)
      }
      if (SST.scale > DecimalType.MAX_SCALE) {
        throw new IllegalArgumentException("SST " + SSA + "小数位数超过最大长度" + DecimalType.MAX_SCALE)
      }
      val dfT = countsEachColumn.sum - 1 // 总体自由度

      val dfA = columnName.size - 1 // 组间自由度
      val dfE = dfT - dfA // 组内自由度
      val MSE = SSE / dfE // 组内误差平均值

      val MSA = SSA / dfA // 组间误差平均值
      val FValue = MSA / MSE // F比

      val fDistribution = new FDistribution(dfA, dfE)
      val pValue = 1 - fDistribution.cdf(FValue.doubleValue())

      val FCriterion = fDistribution.inverseCdf(1 - alphaValue) //  标准值，也就是F分布表中的值

      val rowList = new java.util.ArrayList[Row]()

      rowList.add(RowFactory.create(Double.box(SST.doubleValue()), Long.box(dfT), Double.box(SSE.doubleValue()),
        Long.box(dfE), Double.box(MSE.doubleValue()), Double.box(SSA.doubleValue()), Long.box(dfA), Double.box(MSA.doubleValue()),
        Double.box(FValue.doubleValue()), Double.box(pValue), Double.box(FCriterion)))

      dataFrame.sparkSession.createDataFrame(rowList, getStructType).as("OneWayAnova")

    }catch {
      case e:Throwable =>{
        val errorMessage = e.getMessage
        log.error(errorMessage)
        throw new RuntimeException(e)
      }
    }
  }

  /**
    * SST, dfT,  SSE, dfE , MSE , SSA , dfA, MSA , F , pValue ,FCriterion
    * double,Long,double,Long,double,double,Long,double,double,double,double
    *
    * 老板要求出参由decimal改为double
    * @return
    */
  def getStructType(): StructType = {

    val fields = new Array[StructField](11)

    fields.update(0, new StructField("SST", DoubleType))
    fields.update(1, new StructField("dfT", LongType))
    fields.update(2, new StructField("SSE", DoubleType))
    fields.update(3, new StructField("dfE", LongType))
    fields.update(4, new StructField("MSE", DoubleType))
    fields.update(5, new StructField("SSA",DoubleType))
    fields.update(6, new StructField("dfA", LongType))
    fields.update(7, new StructField("MSA", DoubleType))
    fields.update(8, new StructField("F", DoubleType))
    fields.update(9, new StructField("pValue",DoubleType))
    fields.update(10, new StructField("FCriterion", DoubleType))

    return new StructType(fields)
  }

}
