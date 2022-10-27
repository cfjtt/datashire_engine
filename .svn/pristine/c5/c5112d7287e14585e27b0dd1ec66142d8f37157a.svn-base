package com.eurlanda.datashire.engine.spark.mllib.classification

import com.eurlanda.datashire.engine.util.ExceptionUtil
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel, RandomForestClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  *  朴素贝叶斯分类
  *  要求:
  *  1) 标签从0开始， 0,1,2,3，...
  *  2）所有特征值 >=0
  *  3) model是bernoulli时，特征值要是{0，1}
  *
  *
  */
class NaiveBayesClassifiersSquid {

  var trainDataPercentage = 0.8
  var modelTypeIndex = -1
  var smoothingParameter = 0.0 //光滑参数
  var thresholdsCsn = "" // 阈值
  var maxCategories = 100 // 由于朴素贝叶斯分类标签数最大100，所有当大于100时，需要设置为特征离散阈值

  private val log = LogFactory.getLog(classOf[NaiveBayesClassifiersSquid])

  def run(dataFrame: DataFrame): (NaiveBayesModel, java.util.HashMap[String, Any]) = {
    require(smoothingParameter >= 0.0, "光滑参数应大于会等于0")
    require(trainDataPercentage >= 0.0 && trainDataPercentage <= 1.0, "训练集比例应在区间( 0,100 ]")

    val dataFrameProcessedNa = dataFrame.na.drop().persist(StorageLevel.MEMORY_AND_DISK)
    if (dataFrameProcessedNa.rdd.isEmpty()) {
      throw new RuntimeException("朴素贝叶斯分类没有训练数据")
    }
    val dataCount = dataFrameProcessedNa.count()
    if (dataCount == 0) {
      throw new RuntimeException("朴素贝叶斯分类没有训练数据")
    }
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(maxCategories)
      .fit(dataFrameProcessedNa)

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(dataFrameProcessedNa)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    var trainData:DataFrame= null
    var testData:DataFrame= null
    try {
      if(trainDataPercentage == 1.0){
        trainData = dataFrameProcessedNa
        testData = dataFrameProcessedNa
      }else{
        val splited = dataFrameProcessedNa.randomSplit(Array(trainDataPercentage, 1 - trainDataPercentage))
        trainData = splited.apply(0)
        testData = splited.apply(1)
      }
      val naiveBayes = new NaiveBayes()
        .setModelType(getModelType)
        .setSmoothing(smoothingParameter)
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")
        .setThresholds(util.parseClassificationThresholds(thresholdsCsn)) // 分类阈值， 要等于类别标签的数量,全部 >=0 且最多只能有一个0

      val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, naiveBayes, labelConverter))
      val pipelineModel = pipeline.fit(trainData)
      val predictions = pipelineModel.transform(testData)
      val model = pipelineModel.stages(2).asInstanceOf[NaiveBayesModel]

      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")

      var precision = evaluator.evaluate(predictions)
      if (precision.toString.toLowerCase.contains("infinity") || precision.toString.toLowerCase.contains("nan")) {
        precision = 1.0
      }
      val f1 = evaluator.setMetricName("f1").evaluate(predictions)
      val weightedPrecision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
      val weightedRecall = evaluator.setMetricName("weightedRecall").evaluate(predictions)

      val modelMetrics = new java.util.HashMap[String, Any]()
      modelMetrics.put("dataCount", dataCount)
      modelMetrics.put("precision", precision)
      modelMetrics.put("f1", f1)
      modelMetrics.put("weightedPrecision", weightedPrecision)
      modelMetrics.put("weightedRecall", weightedRecall)
      (model, modelMetrics)
    } catch {
      case e: Throwable => {
        log.error("NaiveBayesClassifiersSquid 异常:" + e.getMessage)
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

  /**
    * modelTypeIndex 从0开始
    * @return
    */
  private def getModelType(): String = {
    val modelTypes = Array("multinomial", "bernoulli")
    require(modelTypeIndex >= 0 && modelTypeIndex < modelTypes.length, "模型类型应是" + modelTypes.mkString(","))
    modelTypes.apply(modelTypeIndex)
  }

}
