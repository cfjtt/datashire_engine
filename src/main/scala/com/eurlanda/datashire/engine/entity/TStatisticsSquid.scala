package com.eurlanda.datashire.engine.entity

import java.util

import com.eurlanda.datashire.engine.spark.mllib.dimensionareduct.PrincipalComponentAnalysis
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid
import com.eurlanda.datashire.engine.spark.statistics.StatisticsAlgorithmName.StatisticsAlgorithmName
import com.eurlanda.datashire.engine.spark.statistics.anova.OneWayAnova
import com.eurlanda.datashire.engine.spark.statistics.timeseries.MovingAverage
import com.eurlanda.datashire.engine.spark.statistics.{StatisticsAlgorithmName, _}
import org.apache.commons.logging.LogFactory
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

class TStatisticsSquid extends  TSquid   {

  private val log = LogFactory.getLog(classOf[TStatisticsSquid])
  setType(TSquidType.STATISTICS_SQUID)

  var previousSquid: TSquid = null
  var squidName: String = null
  var statisticsAlgorithmName: StatisticsAlgorithmName = null
  var params: util.HashMap[String, Object] = null
  //控制参数
  var previousId2Columns: util.Map[Integer, TStructField] = null
  var currentId2Columns: util.Map[Integer, TStructField] = null

  override def run(jsc: JavaSparkContext): Object = {

    if (statisticsAlgorithmName == null || statisticsAlgorithmName.equals("")) {
      throw new IllegalArgumentException("没有选择统计算法")
    }
    log.info("计算" + statisticsAlgorithmName)

    if (previousId2Columns == null) {
      throw new RuntimeException("统计squid翻译异常")
    }
    val preRDD = previousSquid.getOutRDD.persist(StorageLevel.MEMORY_AND_DISK)
    var dataFrame: DataFrame = null
    if (previousSquid.outDataFrame != null) {
      if(previousSquid.outDataFrame.rdd.isEmpty()){
        throw new RuntimeException("没有数据或数据是null")
      }
      dataFrame = previousSquid.outDataFrame
    } else {
      // dataFrame = TJoinSquid.rddToDataFrame(getJobContext.getSparkSession, squidName, previousSquid.getOutRDD.rdd, previousId2Columns)
      if(previousSquid.getOutRDD.rdd.isEmpty()){
        throw new RuntimeException("没有数据或数据是null")
      }
      dataFrame = rddToDataFrame(getJobContext.getSparkSession, squidName, preRDD, previousId2Columns)
    }
    try {
      val columnNames = dataFrame.columns
      if (statisticsAlgorithmName.equals(StatisticsAlgorithmName.STDDEV)
        || statisticsAlgorithmName.equals(StatisticsAlgorithmName.KURTOSIS)
        || statisticsAlgorithmName.equals(StatisticsAlgorithmName.PERCENTILE)) {
        if (columnNames == null || columnNames.size != 1) {
          throw new IllegalArgumentException(statisticsAlgorithmName + "的入参不是一列")
        }
        val statistics = new Input1ColumnOutput1Value(statisticsAlgorithmName, params)
        outDataFrame = statistics.run(dataFrame)
      } else if (statisticsAlgorithmName.equals(StatisticsAlgorithmName.MOVINGAVERAGE)) {
        if (columnNames == null || columnNames.size != 1) {
          throw new IllegalArgumentException(statisticsAlgorithmName + "的入参不是一列")
        }
        outDataFrame = MovingAverage.run(dataFrame, params)
      } else if (statisticsAlgorithmName.equals(StatisticsAlgorithmName.CORRELATION)) {
        if (columnNames == null || columnNames.size != 2) {
          throw new IllegalArgumentException(statisticsAlgorithmName + "的入参不是两列")
        }
        val statistics = new Input2ColumnOutput1Value(statisticsAlgorithmName, params)
        outDataFrame = statistics.run(dataFrame)
      } else if (statisticsAlgorithmName.equals(StatisticsAlgorithmName.ONEWAYANOVA)) {
        outDataFrame = OneWayAnova.run(dataFrame, params)
      } else if (statisticsAlgorithmName.equals(StatisticsAlgorithmName.PRINCIPAL_COMPONENT_ANALYSIS)) {
        outDataFrame = PrincipalComponentAnalysis.run(params, dataFrame)
      } else {
        throw new RuntimeException("不存在统计算法" + StatisticsAlgorithmName)
      }
      outRDD = TJoinSquid.groupTaggingDataFrameToRDD(outDataFrame, currentId2Columns).toJavaRDD
      return outRDD
    } catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error(e.getStackTrace())
        if(errorMessage.contains("Cannot compute the covariance of a RowMatrix with <= 1 row")) {
          log.error("相关系数至少有2行数据")
          throw new IllegalArgumentException("相关系数至少有2行数据")
        }else if(errorMessage.contains("empty collection")) {
          log.error("没有数据或数据是null")
          throw new IllegalArgumentException("相关系数至少有2行数据")
        } else if(errorMessage.contains("The size of BLOB/TEXT data inserted in one transaction is greater than 10% of redo log size")){
          throw new RuntimeException("数据大小超过日志文件的10%，请在mysql的配置文件my.cnf中增加变量innodb_log_file_size的值", e)
        } else {
          log.error("计算" + statisticsAlgorithmName + "异常:" + errorMessage)
          throw e
        }
      }
    }finally {
      if(preRDD != null){
        preRDD.unpersist()
      }
    }

  }

  /**
    *
    * @param session
    * @param squidName
    * @param rdd
    * @param id2Columns
    * @return ( ,最大小数位数)
    */
  def rddToDataFrame(session: SparkSession, squidName: String, rdd: RDD[java.util.Map[Integer, DataCell]], id2Columns: java.util.Map[Integer, TStructField]): DataFrame = {
    val ids = id2Columns.keySet().toArray(Array[Integer]()).sorted.toSeq
    // 最多有几位小数，几位小数,如果没有小数，则转换为bigdecimal会是null值
    val scalaMaxSize = rdd.mapPartitions(par=>{
      val idstmp = ids
      par.map(line=>{
        idstmp.map(id => {
          val datatmp = line.get(id)
          if (datatmp.getdType().equals(TDataType.BIG_DECIMAL) && datatmp.getData!= null) {
            val dotIdx = datatmp.getData.toString.lastIndexOf(".")
            if (dotIdx != -1) {
              //几位小数,如果没有小数，则转换为bigdecimal会是null值
              datatmp.getData.toString.length - 1 - dotIdx
            }else {
              0
            }
            } else {
            0
          }
          }).max // 每行的小数位最大值
      })
    }).max

    val data = rdd.map(m => {
      val ids1 = ids
      val rowArray = collection.mutable.ArrayBuffer[Any]()
      for (id <- ids1) {
        val data = m.get(id)
        if (data != null) {
          rowArray += m.get(id).getData
        } else {
          rowArray += null
        }
      }
      Row.fromSeq(rowArray)
    })

    val id2ColumnsTmp = new java.util.HashMap[Integer, TStructField]()
    val iter = id2Columns.entrySet().iterator()
    while (iter.hasNext()) {
      val entry = iter.next()
      val key = entry.getKey().toInt
      val value = entry.getValue()
      value.setScale(scalaMaxSize)
      id2ColumnsTmp.put(key, value)
    }
    session.createDataFrame(data, TJoinSquid.genStructType(id2ColumnsTmp)).as(squidName)
  }

}
