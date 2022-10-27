package com.eurlanda.datashire.engine.spark.mllib.classification

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2017-08-10.
  * 逻辑回归
  */
class LogisticRegressionSquid extends Serializable {

  private val log: Log = LogFactory.getLog(classOf[LogisticRegressionSquid])

  var maxIter = -1
  var regParam = -1.0
  var elasticNetParam = -1.0
  var familyIndex = -1
  var fitIntercept = true
  var standardization = true
  var tolerance = -0.1
  var trainingDataPercentage = -1.0
  var thresholdsCsn = "" //  分类阈值， 要等于类别标签的数量,全部 >=0 且最多只能有一个0

  def run(dataFrame: DataFrame): ( LogisticRegressionModel,java.util.HashMap[String,Any]) = {

    require(elasticNetParam >= 0.0 && elasticNetParam <= 1.0, "弹性网参数应在区间[0,1]")
    require(maxIter >= 0, "迭代次数应大于会等于0")
    require(regParam >= 0, "正则化参数应大于会等于0")
    require(tolerance >= 0.0, "容许误差应大于会等于0")
    require(trainingDataPercentage >= 0.0 && trainingDataPercentage <= 1.0, "训练集比例应在区间( 0,100 ]")

    val dataFrameProcessedNa = dataFrame.na.drop().persist(StorageLevel.MEMORY_AND_DISK)
    if (dataFrameProcessedNa.rdd.isEmpty()) {
      throw new RuntimeException("逻辑回归没有训练数据")
    }
    val dataCount = dataFrameProcessedNa.count()
    if (dataCount == 0) {
      throw new RuntimeException("逻辑回归没有训练数据")
    }
    try {
      var trainingDf: DataFrame = null
      var testDf: DataFrame = null
      if (trainingDataPercentage == 1.0) {
        trainingDf = dataFrameProcessedNa
        testDf = dataFrameProcessedNa
      } else {
        val splitedDf = dataFrameProcessedNa.randomSplit(Array(trainingDataPercentage, 1 - trainingDataPercentage))
        trainingDf = splitedDf.apply(0)
        testDf = splitedDf.apply(1)
      }

      if (dataFrameProcessedNa.rdd.isEmpty()) {
        throw new RuntimeException("逻辑回归没有训练数据,可能原因：1）数据量太少；2）训练集比例设置过大好或过小")
      }
      val labelCount= trainingDf.select("label").dropDuplicates("label").collect().length  // 类别数
      val lr = new LogisticRegression()
        .setMaxIter(maxIter)
        .setRegParam(regParam)
        .setElasticNetParam(elasticNetParam)
        .setFamily(getFamily)
        .setFitIntercept(fitIntercept)
        .setStandardization(standardization)
        .setTol(tolerance)
        if(labelCount ==1) { //  只有一类, 分类阈值只有一个元素，且在区间[0, 1]
          val threshold = util.parseClassificationThresholds(thresholdsCsn)
          if(threshold.length != 1 || threshold.apply(0) < 0.0 || threshold.apply(0) >1.0) {
            throw new RuntimeException("只有一个标签值时的分类阈值元素个数要等于 1 且在区间 [0 , 1]")
          }
          lr.setThreshold(threshold.apply(0))
        }else {
          lr.setThresholds(util.parseClassificationThresholds(thresholdsCsn))
        }

      val lrModel = lr.fit(trainingDf)

      val predictions = lrModel.transform(testDf)
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
      var precision = evaluator.evaluate(predictions)
      if (precision.toString.toLowerCase.contains("infinity") || precision.toString.toLowerCase.contains("nan")) {
        precision = 1.0
      }
      evaluator.setMetricName("f1")
      val f1 = evaluator.evaluate(predictions)

      evaluator.setMetricName("weightedPrecision")
      val weightedPrecision = evaluator.evaluate(predictions)

      evaluator.setMetricName("weightedRecall")
      val weightedRecall = evaluator.evaluate(predictions)

      val modelMetrics = new java.util.HashMap[String,Any]()
      modelMetrics.put("dataCount",dataCount)
      modelMetrics.put("precision",precision)
      var totalIterations = maxIter
      if(lrModel.hasSummary){
        totalIterations = math.min(lrModel.summary.totalIterations,maxIter)
      }
      modelMetrics.put("totalIterations",totalIterations)
      modelMetrics.put("f1",f1)
      modelMetrics.put("weightedPrecision",weightedPrecision)
      modelMetrics.put("weightedRecall",weightedRecall)
      (lrModel,modelMetrics)
    } catch {
      case e: Throwable => {
        log.error("逻辑回归训练异常：" + e)
        val errmsg = e.getMessage
        log.error(errmsg)
        log.error(e.getStackTrace())
        if (errmsg.toLowerCase().contains("but was given by empty one")) {
          throw new RuntimeException("没有训练数据，可能原因：1)输入数据太少；2)训练集比例设置过大或过小", e)
        } else if (errmsg.toLowerCase().contains("nothing has been added to this summarizer")) {
          throw new RuntimeException("没有测试数据，可能原因：1)输入数据太少；2)训练集比例设置过大或过小", e)
        } else if(errmsg.contains("only applies to binary classification, but thresholds has length != 2")){
          throw new RuntimeException("二分类的分类阈值元素个数要等于1", e)
        }else if (errmsg.contains("non-matching numClasses and thresholds.length")) {
          var firstNumberGroup = util.getFirstNumberGroup(errmsg)
          if(firstNumberGroup == null){
            firstNumberGroup = ""
          }
          throw new RuntimeException("分类阈值元素个数要等于分类数量" + firstNumberGroup, e)
        } else if (errmsg.contains("Binomial family only supports 1 or 2 outcome classes but found")) {
          throw new RuntimeException("Binomial分布族只适应于二分类", e)
        } else if (errmsg.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源导致任务丢失，请稍后重试或在配置文件中调大配置项：" +
            "SPARK_EXECUTOR_MEMORY，SPARK_DRIVER_MEMORY", e)
        }
        throw e
      }
    } finally {
      dataFrameProcessedNa.unpersist()
    }
  }


  private def getFamily: String = {
    //familyIndex 从1开始
    val familys = Array("auto", "binomial", "multinomial")
    require(familyIndex >= 1 && familyIndex <= familys.length)
    familys.apply(familyIndex - 1)
  }



}
