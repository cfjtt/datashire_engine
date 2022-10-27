package com.eurlanda.datashire.engine.spark.mllib.classification

import com.eurlanda.datashire.engine.util.ExceptionUtil
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * SVM目前只能是二分类，标签是0,1
  */
class SVMSquid {

  var miniBatchFraction = 1.0 // 每次最小数据量
  var numIterations = 10 // 迭代次数
  var stepSize = 0.1
  var regParam = 0.0
  var thresholdsCsn = "" // 只有一个值
  var trainingDataPercentage = 1.0

  private val log  = LogFactory.getLog(classOf[SVMSquid])

  def run(dataRDD:RDD[LabeledPoint]): (SVMModel,java.util.HashMap[String, Any]) = {
     require(miniBatchFraction > 0.0 && miniBatchFraction<=1.0, "每次最小数据量应在区间(0,1]")
     require(numIterations > 0 , "迭代次数应大于0")
     require(stepSize > 0 , "步长应大于0")
     require(trainingDataPercentage >= 0.0 && trainingDataPercentage <= 1.0, "训练集比例应在区间( 0,100 ]")
     require(regParam >= 0.0 && regParam <= 1.0, "正则化参数应在区间[0,1]")
     require(thresholdsCsn != null  && thresholdsCsn.trim !="","分类阈值不能是null或空字符串")

    val dataFrameProcessedNa = dataRDD.persist(StorageLevel.MEMORY_AND_DISK)
    if (dataFrameProcessedNa.isEmpty()) {
      throw new RuntimeException("支持向量机分类没有训练数据")
    }
    val dataCount = dataFrameProcessedNa.count()
    if (dataCount == 0) {
      throw new RuntimeException("支持向量机分类没有训练数据")
    }
    try {
      var trainingData:RDD[LabeledPoint]= null;
      var testData:RDD[LabeledPoint]= null;
      if(trainingDataPercentage == 1.0){
        trainingData = dataFrameProcessedNa
        testData = dataFrameProcessedNa
      }else {
        val splited = dataFrameProcessedNa.randomSplit(Array(trainingDataPercentage, 1 - trainingDataPercentage))
        trainingData = splited.apply(0)
        testData = splited.apply(1)
      }
      val model = SVMWithSGD
        .train(trainingData, numIterations, stepSize, regParam, miniBatchFraction)
        .setThreshold(thresholdsCsn.trim.toDouble)

      val scoreAndLabels = testData.map { point =>
        val score = model.predict(point.features)
        (score, point.label)
      }

      var precision = 1.0
      val correctCount = scoreAndLabels.map(x=> if(x._1 == x._2) 1.0 else 0.0).reduce(_+_)
      precision = correctCount/testData.count()
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)

      val modelMetrics = new java.util.HashMap[String, Any]()
      modelMetrics.put("dataCount", dataCount)
      modelMetrics.put("precision", precision)
      modelMetrics.put("numBins", metrics.numBins)
      modelMetrics.put("areaUnderROC", metrics.areaUnderROC())
      modelMetrics.put("areaUnderPR", metrics.areaUnderPR())

      (model, modelMetrics)
    } catch {
      case e: Throwable => {
        log.error("SVMSquid 异常:" + e.getMessage)
        log.error(e.getStackTrace())
        val exp = ExceptionUtil.translateExceptionMessage(e)
        if(exp != null){
          throw exp
        }
        throw e
      }
    } finally {
      dataFrameProcessedNa.unpersist()
    }

  }

}
