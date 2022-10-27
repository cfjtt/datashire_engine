package com.eurlanda.datashire.engine.spark.mllib.cluster

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  * http://blog.csdn.net/jwh_bupt/article/details/7685809
  * http://shiyanjun.cn/archives/1388.html
  * http://blog.csdn.net/lipengcn/article/details/50320893
  *
  * 如果一个聚类（包括根聚类）中的样本数量比值minDivisibleClusterSize小，则该聚类不再分隔
  * 最多分隔63次
  *
  */
class BisectingKMeansSquid {

  private val log: Log = LogFactory.getLog(classOf[BisectingKMeansSquid])

  var k = 4 // 叶聚类数量
  var maxIterations = 10
  var minDivisibleClusterSize = 1.0 // 可分聚类最小样本数,// >0 ,当 >=1 时表示 最小数量，<1 时表示最小比例
  var trainDataPercentage = 1.0 // 全部用于训练，

  /**
    *
    * @param dataFrame 只有一列, 每行都是一个vector
    * @return
    */
  def run(dataFrame: DataFrame): (BisectingKMeansModel, java.util.HashMap[String, Any]) = {

    require(k >= 2, "K值应大于或等于2")
    require(maxIterations > 0, "迭代次数应大于0")
    require(minDivisibleClusterSize > 0.0, "可分聚类最小样本数应大于0")
    require(trainDataPercentage > 0.0 && trainDataPercentage <= 1.0, "训练集比例应在区间 ( 0,100 ]")

    if (dataFrame == null ) {
      throw new RuntimeException("没有训练数据")
    }
    val precessedNaDf = dataFrame.na.drop().persist(StorageLevel.MEMORY_AND_DISK)
    val dataCount = precessedNaDf.count()
    if (dataCount ==0 ) {
      throw new RuntimeException("没有训练数据")
    }
    var trainDf: DataFrame = null
    var testDf: DataFrame = null
    if (trainDataPercentage.equals(1.0)) {
      trainDf = precessedNaDf
      testDf = precessedNaDf
    } else {
      val splitedDf = precessedNaDf.randomSplit(Array(trainDataPercentage, 1 - trainDataPercentage))
      trainDf = splitedDf.apply(0)
      testDf = splitedDf.apply(1)
    }

    val bisectingKMeans = new BisectingKMeans()
    bisectingKMeans.setK(k)
    bisectingKMeans.setMaxIter(maxIterations)
    bisectingKMeans.setMinDivisibleClusterSize(minDivisibleClusterSize)
    try {
      val model = bisectingKMeans.fit(trainDf)
      val SSE = model.computeCost(testDf)
      val modelMetrics = new java.util.HashMap[String, Any]()
      modelMetrics.put("dataCount", dataCount)
      modelMetrics.put("SSE", SSE)

      (model, modelMetrics)
    } catch {
      case e: Throwable => {
        val msg = e.getMessage
        log.error(msg)
        e.printStackTrace()
        if (msg.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源导致任务丢失，请稍后重试或调大K值或调大最小样本数", e)
        }else if (msg.contains("Requested array size exceeds VM limit")) {
          throw new RuntimeException("数组大小超过java虚拟机限制，可能原因：属性参数设置过大", e)
        }
        throw e
      }
    } finally {
      if (precessedNaDf != null) {
        precessedNaDf.unpersist()
      }
    }

  }

}
