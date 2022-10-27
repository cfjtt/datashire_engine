package com.eurlanda.datashire.engine.spark.statistics.timeseries

import java.util

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, _}


/**
  * Created by Administrator on 2017-05-09.
  */
object MovingAverage extends Serializable {

  val outputColumnName = "MovingAverage"
  private val log: Log = LogFactory.getLog(MovingAverage.getClass)

  /**
    * 移动平均
    *
    * @param dataFrame
    * @param params 移动步长
    * @return
    */
  def run(dataFrame: DataFrame, params: util.HashMap[String, Object]): DataFrame = {
  //  log.info("移动平均1")
    try {
      val columnName = dataFrame.columns.apply(0)
      val dataFrameProcessNa = dataFrame.na.drop()
      if(dataFrameProcessNa == null || dataFrameProcessNa.rdd.isEmpty()){
        throw new IllegalArgumentException("移动平均算法数据是null或空字符串,可能原因：decimal的scale设置过大")
      }
      val count = dataFrameProcessNa.count()
      if (params == null) {
        throw new IllegalArgumentException("移动平均算法的参数MoveStep不能是null或空字符串")
      }
      if (count == 0) {
        throw new IllegalArgumentException("移动平均算法的入参不能是null或空字符串")
      }
      val moveStep = params.get("MoveStep")
      if (moveStep == null || moveStep.toString.equals("")) {
        throw new IllegalArgumentException("移动平均算法的参数MoveStep不能是null或空字符串")
      }
      val colmunes = dataFrame.columns
      if (colmunes.size != 1) {
        throw new IllegalArgumentException("输入数据不是1列")
      }
      var moveStepValue = 0L
      try {
        moveStepValue = moveStep.toString.toLong
      } catch {
        case _: Exception => throw new IllegalArgumentException("移动平均算法的参数MoveStep不是正整数")
      }
      if (moveStepValue <= 0) {
        throw new IllegalArgumentException("移动平均算法的参数MoveStep不是正整数")
      }

      if (dataFrameProcessNa.count() == 1) {
        // 只有一行
        val rowList = new java.util.ArrayList[Row]()
        val data = dataFrameProcessNa.collect().take(1).apply(0).getDecimal(0)
        if (data.precision() > DecimalType.MAX_PRECISION) {
          throw new IllegalArgumentException(data + "超过最大长度" + DecimalType.MAX_PRECISION)
        }
        if (data.scale() > DecimalType.MAX_SCALE) {
          throw new IllegalArgumentException(data + "小数位数超过最大长度" + DecimalType.MAX_SCALE)
        }
        rowList.add(RowFactory.create(data))
        val fields = new Array[StructField](1)
        fields.update(0, new StructField("MovingAverage", DecimalType(data.precision(), data.scale())))
        val structType = StructType(fields)
        return dataFrameProcessNa.sparkSession.createDataFrame(rowList, structType)
      }

      if (moveStepValue >= count) {
        return dataFrameProcessNa.select(columnName).agg(Map(columnName -> "avg")).toDF(outputColumnName)
      }
    //  log.info("移动平均2")
      //添加自增列id
      val columnNameId = columnName + "ID"
      //  var iddf = dataFrameProcessNa.withColumn(columnNameId, org.apache.spark.sql.functions.monotonically_increasing_id()) // 不是连续的
      val iddf = addIdColumn(dataFrameProcessNa, columnNameId)
   //   log.info("移动平均3")
      val rowList = new java.util.ArrayList[Row]()
      for (i <- 0L to (count - moveStepValue)) {
        val j = i + moveStepValue
        val filtereddf = iddf.filter(columnNameId + ">=" + i).filter(columnNameId + "<" + j).select(columnName)
        val avgtmp = filtereddf.agg(Map(columnName -> "avg")).head(1).apply(0).getDouble(0)
        rowList.add(RowFactory.create(Double.box(avgtmp)))
      }
   //   log.info("移动平均4")
      val fields = new Array[StructField](1)
      fields.update(0, new StructField(outputColumnName, DoubleType))
      val structType = StructType(fields)
      dataFrameProcessNa.sparkSession.createDataFrame(rowList, structType).as("MovingAverage")
    }catch {
      case e:Throwable =>{
        val errorMessage = e.getMessage
        log.error(errorMessage)
        throw new RuntimeException(e)
      }
    }
  }

  /**
    * 添加id列
    *
    * @param df 只有一列
    * @return
    */
  def addIdColumn(df: DataFrame, idColumnName: String): DataFrame = {

    val resdf = df.rdd.zipWithIndex().map(row => {
      RowFactory.create(Long.box(row._2), Double.box(row._1.getDecimal(0).doubleValue()))
    })
    val fields = new Array[StructField](2)
    fields.update(0, new StructField(idColumnName, LongType))
    fields.update(1, new StructField(df.columns.apply(0), DoubleType))

    val structType = StructType(fields)
    df.sparkSession.createDataFrame(resdf, structType)

  }

}
