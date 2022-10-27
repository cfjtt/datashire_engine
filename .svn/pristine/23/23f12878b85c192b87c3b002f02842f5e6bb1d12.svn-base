package com.eurlanda.datashire.engine.spark.mllib.cluster

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  *
  * http://blog.csdn.net/u012102306/article/details/52212870
  */
class KMeansSquid {

  var k = 0
  var tolerance = 0.0
  var maxIterations = 0
  var initializationMode = "" // k-means||,random
  var initSteps = 0 //  k-means|| 才可用

  private val log: Log = LogFactory.getLog(classOf[KMeansSquid])

  /**
    *
    * @param dataframe
    * @return
    */
  def run(dataframe:DataFrame): (KMeansModel,java.util.HashMap[String, Any]) = {

    require(k >= 2, "K值应大于或等于2")
    require(tolerance >= 0.0, "容许误差大于或等于0")
    require(maxIterations > 0, "迭代次数应大于0")
    require(initializationMode.trim.equals("k-means||")
      || initializationMode.trim.equals("random"), "初始化模型应是k-means||,random")

    if (dataframe == null ) {
      throw new RuntimeException("没有训练数据")
    }
    val df = dataframe.na.drop().persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val dataCount = df.count()
      if (dataCount == 0) {
        throw new RuntimeException("K-Means没有训练数据")
      }
      val kmeans = new KMeans()
        .setK(k)
        .setTol(tolerance)
        .setMaxIter(maxIterations)
        .setInitMode(initializationMode)
      if (initializationMode.equalsIgnoreCase("k-means||")) {
        require(initSteps > 0, "初始步数值应大于0")
        kmeans.setInitSteps(initSteps) // >0    k-means|| 才可用
      }

      val model = kmeans.fit(dataframe)
      val SSE = model.computeCost(dataframe)
      val modelMetrics = new java.util.HashMap[String, Any]()
      modelMetrics.put("dataCount", dataCount)
      modelMetrics.put("SSE", SSE)

      (model, modelMetrics)

    } catch {
      case e: Throwable => {
        val errMsg = e.getMessage
        log.error(errMsg)
        e.printStackTrace()
        if(errMsg.contains("Size exceeds Integer.MAX_VALUE")) {
          throw new RuntimeException("数据分区超过最大限制，请稍后重试")
        }
        throw e
      }
    } finally {
      if (df != null) {
        df.unpersist()
      }
    }

  }

}
