package com.eurlanda.datashire.engine.spark.mllib.dimensionareduct

import java.util

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, _}


/**
  * Created by Administrator on 2017-05-24.
  */
object PrincipalComponentAnalysis  extends Serializable {

  private val log: Log = LogFactory.getLog(PrincipalComponentAnalysis.getClass)

  /**
    *  k :主成分数 ， 大于或等于1，且小于或等于PCAInput的列数
    * @param  dataFrame 每行都是csn
    * @return  第一列是 PrincipalComponent，第二列是 ExplainedVariance
    */
  def run(params: util.HashMap[String, Object],dataFrame:DataFrame): DataFrame = {

    if (params == null) {
      throw new IllegalArgumentException("主成分分析没有输入参数")
    }
    var principalComponentCount = -1
    try {
      principalComponentCount = params.get("k").toString.toInt
    } catch {
      case e: Exception => throw new IllegalArgumentException("主成分分析的参数k不是整数")
    }
    val dataFrameProcessNa = dataFrame.na.drop().persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    try {
      if (dataFrameProcessNa == null || dataFrameProcessNa.rdd.isEmpty()) {
        throw new IllegalArgumentException("主成分分析数据是null或空字符串,可能原因：decimal的scale设置过大")
      }
      val columnName = dataFrameProcessNa.columns
      //   val columnCount = dataFrameProcessNa.length
      if (columnName.length < 1) {
        throw new IllegalArgumentException("主成分分析没有入参")
      }
      if (principalComponentCount <= 0 || principalComponentCount > columnName.length) {
        throw new IllegalArgumentException("主成分分析的参数k应在区间 [ 1, " + columnName.length + " ]")
      }
      //把多列合并成一列 Vector
      val inputcolname = columnName.mkString("")
      val vectorDf = df2Vector(dataFrameProcessNa, inputcolname)

      //   val scaler = new StandardScaler()
      //    .setInputCol(inputcolname)
      //   .setOutputCol("StandardScaler")
      //   .setWithStd(true)
      //   .setWithMean(true)
      //  val scalerModel = scaler.fit(vectorDf)
      //  val scaledDf = scalerModel.transform(vectorDf)

      val outputColName = "PrincipalComponent"
      val pca = new PCA()
        .setInputCol(inputcolname)
        .setOutputCol(outputColName)
        .setK(principalComponentCount)

      val model = pca.fit(vectorDf)
      val resultPrincipalComponent = model.transform(vectorDf).select(outputColName)

      val pcvar = resultPrincipalComponent.rdd.mapPartitions(par => {
        par.map(row => {
          val elements = collection.mutable.ArrayBuffer[Any]()
          val exp = model.explainedVariance.values.mkString(",")
          elements.append(row.get(0).toString.replace("[", "").replace("]", "")) // 去掉前后的[ ]，转成csn
          elements.append(exp)
          Row.fromSeq(elements)
        })
      })

      val res = dataFrameProcessNa.sparkSession.createDataFrame(pcvar.map(row => (row.get(0).toString, row.get(1).toString)))
        .toDF("PrincipalComponent", "ExplainedVariance")
      return res
    } catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("PrincipalComponentAnalysis异常:" + errorMessage)
        log.error("PrincipalComponentAnalysis异常:" + e.printStackTrace())
        if (errorMessage.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源导致任务丢失，请稍后重试或在配置文件中调大配置项：" +
            "SPARK_EXECUTOR_MEMORY，SPARK_DRIVER_MEMORY", e)
        }
        throw e
      }
    } finally {
      dataFrameProcessNa.unpersist()
    }

  }

  /**
    *  把多列合并成一列 Vector， 调用前需要处理null等非数值
    * @param dataFrame
    * @return
    */
 def df2Vector(dataFrame:DataFrame,outputColumnName: String): DataFrame ={
   val columnName = dataFrame.columns
   val columnCount = columnName.length
   val vectorRdd = dataFrame.rdd.mapPartitions(par => {
     par.map { row => {
       val elements = collection.mutable.ArrayBuffer[Double]()
       val colclount = columnCount
       for (i <- 0 until (colclount)) {
         elements.append(row.getDecimal(i).doubleValue())
       }
       Vectors.dense(elements.toArray)
     } }
   })
   val vectorDf = dataFrame.sparkSession.createDataFrame(vectorRdd.map(Tuple1.apply)).toDF(outputColumnName)
   return vectorDf
 }


}
