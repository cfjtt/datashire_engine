package com.eurlanda.datashire.engine.spark.statistics

import java.util

import com.eurlanda.datashire.engine.spark.statistics.StatisticsAlgorithmName.StatisticsAlgorithmName
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, RowFactory}

/**
  * Created by Administrator on 2017-05-09.
  * 输入两列，输出1个数
  * 相关系数，目前输出不支持decimal类型
  *
  */
class Input2ColumnOutput1Value ( algorithmName:StatisticsAlgorithmName ,params: util.HashMap[String, Object]) extends Serializable {

  private val log: Log = LogFactory.getLog(classOf[Input2ColumnOutput1Value])

  def run(dataFrame:DataFrame): DataFrame = {

    try {
      if (algorithmName == null || algorithmName.equals("")) {
        throw new IllegalArgumentException("统计算法名称是null或空字符串")
      }
      if (dataFrame == null || dataFrame.rdd.isEmpty()) {
        throw new IllegalArgumentException(algorithmName + "数据是null或空字符串,可能原因：decimal的scale设置过大")
      }
      if (algorithmName.equals(StatisticsAlgorithmName.CORRELATION)) {
      //  log.info("CORRELATION_111")
        if (params == null) {
          throw new IllegalArgumentException("相关系数的计算方法是null或空字符串")
        }
        if(dataFrame == null || dataFrame.rdd.isEmpty()){
          throw new RuntimeException("没有数据或数据是null")
        }
        import dataFrame.sparkSession.implicits._
        val tmpnadf = dataFrame.na.drop()
        if(tmpnadf.rdd.isEmpty() ){
          throw new RuntimeException("没有数据或数据是null")
        }
      //  log.info("CORRELATION_222")
        val rdd1 = tmpnadf.mapPartitions(par => {
          par.map(x => x.getDecimal(0).doubleValue())
        }).rdd
      //  log.info("CORRELATION_333")
        val rdd2 = tmpnadf.mapPartitions(par => {
          par.map(x => x.getDecimal(1).doubleValue())
        }).rdd
     //   log.info("CORRELATION_444")
        val methodObj = params.get("Method")
        if (methodObj == null || methodObj.toString.trim().equals("")) {
          throw new IllegalArgumentException("相关系数的计算方法不能是null或空字符串")
        }
        val method = methodObj.toString.trim()
        val methodsArr = Array("pearson", "spearman")
        if (!methodsArr.contains(method)) {
          throw new IllegalArgumentException("相关系数的计算方法应该是" + methodsArr.mkString(","))
        }
      //  log.info("CORRELATION_555")
        val rowList = new java.util.ArrayList[Row]()
        val corr = Statistics.corr(rdd1, rdd2, method)
        rowList.add(RowFactory.create(Double.box(corr)))

        val fields = new Array[StructField](1)
        fields.update(0, new StructField("Correlation", DoubleType))
        val structType = StructType(fields)
     //   log.info("CORRELATION_666")
        dataFrame.sparkSession.createDataFrame(rowList, structType).as("Correlation")

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
