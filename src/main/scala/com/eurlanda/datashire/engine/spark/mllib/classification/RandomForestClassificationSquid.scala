package com.eurlanda.datashire.engine.spark.mllib.classification

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2017-05-24.
  * 随机森林分类
  */
class RandomForestClassificationSquid extends Serializable {

  private val log: Log = LogFactory.getLog(classOf[RandomForestClassificationSquid])

  var numberTrees = -1
  var impurityIndex = -1  // impurityIndex 从0 开始,  "entropy" , "gini"
  var featureSubsetStrategyIndex = -1 // featureSubsetStrategyIndex 从0 开始,  auto,all,onethird,sqrt,log2,n
  var featureSubsetNValue  = -1.0  // featureSubsetStrategy选 N 时的值 (0,1] (1,2,3,...
  var maxBins = -1
  var maxDepth = -1
  var minInfoGain = 0.0
  var subsamplingRate = 0.0
  var trainingDataPercentage = 0.0
  var maxCategories = 0  // 特征离散阈值 >=2 ,如果一个特征不同值的个数>maxCategories,则被该特征看做是连续的，否则看作是离散的
  var thresholdsCsn = "" //  类别阈值， 要等于类别标签的数量,全部 >=0 且最多只能有一个0

  /**
    * total_dataset,model,precision,F1,weighted_precision,weighted_recall
    *
    * @param dataFrame
    */
  def run(dataFrame: DataFrame): (RandomForestClassificationModel,java.util.HashMap[String,Any]) = {

    require(numberTrees >= 1, "树的数量应大于或等于1")
    require(maxDepth >= 0 && maxDepth <= 30, "目前树最大深度是30")
    require(minInfoGain >= 0.0, "信息增益应大于或等于 0")
    require(subsamplingRate > 0.0, "采样率应大于0")
    require(trainingDataPercentage > 0.0 && trainingDataPercentage <= 1.0, "训练集应在区间( 0,100 ]")
    require(maxCategories >= 2, "特征离散阈值大于或等于2")
    require(maxBins >= maxCategories, "特征最大数应大于或等于特征离散阈值" + maxCategories)

    if (dataFrame == null || dataFrame.rdd.isEmpty()) {
      throw new RuntimeException("没有数据或数据是空")
    }
    val dataFramePeocessNa = dataFrame.na.drop()
      .persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val dataCount = dataFramePeocessNa.count()
      if (dataCount == 0) {
        throw new RuntimeException("没有数据或数据是空")
      }

      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(maxCategories)
        .fit(dataFramePeocessNa)

      val labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(dataFramePeocessNa)

      val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)

      val randomForestClassifier = new RandomForestClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")
        .setNumTrees(numberTrees)
        .setImpurity(getImpurity())
        .setFeatureSubsetStrategy(getFeatureSubsetStrategy())
        .setMaxBins(maxBins)
        .setMaxDepth(maxDepth)
        .setMinInfoGain(minInfoGain)
        .setSubsamplingRate(subsamplingRate)
        .setThresholds(util.parseClassificationThresholds(thresholdsCsn))

      var trainingData: DataFrame = null
      var testData: DataFrame = null
      if (trainingDataPercentage == 1.0) {
        trainingData = dataFramePeocessNa
        testData = dataFramePeocessNa
      } else {
        val Array(trainingDatatmp, testDatatmp) = dataFramePeocessNa.randomSplit(Array(trainingDataPercentage, 1 - trainingDataPercentage))
        trainingData = trainingDatatmp
        testData = testDatatmp
      }

      val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, randomForestClassifier, labelConverter))
      val pipelineModel = pipeline.fit(trainingData)
      val predictions = pipelineModel.transform(testData)
      val model = pipelineModel.stages(2).asInstanceOf[RandomForestClassificationModel]

      var precision = 0.0
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
      precision = evaluator.evaluate(predictions)
      if (precision.toString.toLowerCase.contains("infinity") || precision.toString.toLowerCase.contains("nan")) {
        precision = 1
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
      modelMetrics.put("f1",f1)
      modelMetrics.put("weightedPrecision",weightedPrecision)
      modelMetrics.put("weightedRecall",weightedRecall)
     (model,modelMetrics)
    } catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("RandomForestClassification异常:" + errorMessage)
        log.error("RandomForestClassification异常:" + e.printStackTrace())
        if (errorMessage.contains("next on empty iterator") ||
          errorMessage.contains("empty.maxBy") ||
          errorMessage.contains("requires size of input RDD > 0, but was given by empty one") ||
          errorMessage.contains("Nothing has been added to this summarizer")) {
          throw new RuntimeException("没有训练数据或测试数据，可能原因：总体或同一个key的数据过少或训练集比例过小或过大", e)
        } else if (errorMessage.toLowerCase().contains("for input string")) {
          throw new RuntimeException("最大特征数太小:" + maxBins, e)
        } else if (errorMessage.contains("to be at least as large as the number of values in each categorical feature")) {
          throw new RuntimeException("训练样本数量和最大特征数都要大于或等于不同特征值的数量，" +
            "请调大训练集比例，或增加训练数据，或删除特征值数过多的特征", e)
        } else if (errorMessage.contains("empty dataset")) {
          throw new RuntimeException("数据是null或空字符串", e)
        } else if (errorMessage.contains("Requested array size exceeds VM limit")) {
          throw new RuntimeException("数组大小超过java虚拟机限制，可能原因：属性参数设置过大", e)
        } else if (errorMessage.contains("Considering remove this and other categorical")) {
          throw new RuntimeException("最大类数至少是每个类别特征数，建议增加训练样本或移除大的类别特征", e)
        } else if (errorMessage.contains("non-matching numClasses and thresholds.length")) {
          var firstNumberGroup = util.getFirstNumberGroup(errorMessage)
          if(firstNumberGroup == null){
            firstNumberGroup = ""
          }
          throw new RuntimeException("分类阈值元素个数要等于分类数量" + firstNumberGroup, e)
        }  else if (errorMessage.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源导致任务丢失，请稍后重试或在配置文件中调大配置项：" +
            "SPARK_EXECUTOR_MEMORY，SPARK_DRIVER_MEMORY", e)
        } else if (errorMessage.contains("requirement failed")) {
          val regEx = "[\\u4e00-\\u9fa5]"
          val p = java.util.regex.Pattern.compile(regEx)
          if (p.matcher(errorMessage).find()) {
            throw new RuntimeException(errorMessage, e)
          }
          throw new RuntimeException("训练迭代错误，可能原因：总体或同一个key的数据过少或训练集比例过小或过大", e)
        }
        throw e
      }
    } finally {
      dataFramePeocessNa.unpersist(false)
    }

  }

  /**
    *
    * @return
    */
  def getImpurity(): String = {
    // impurityIndex 从0开始
    val impurity = Array("entropy", "gini")
    require(impurityIndex >= 0 && impurityIndex < impurity.length,"计算标准应是:" + impurity.mkString(","))
    impurity.apply(impurityIndex)
  }

  /**
    *
    * @return
    */
  def getFeatureSubsetStrategy(): String = {
    // featureSubsetStrategyIndex 从0开始
    val featureSubsetStrategy = Array("auto", "all", "onethird", "sqrt", "log2", "n")
    require(featureSubsetStrategyIndex >= 0 && featureSubsetStrategyIndex < featureSubsetStrategy.length,
      "特征子集策略应是:" + featureSubsetStrategy.mkString(","))
    val res = featureSubsetStrategy.apply(featureSubsetStrategyIndex)
    if (res.equalsIgnoreCase("n")) {
      // 特征子集比例的取值范围应在(0,1] 或大于1的整数
      require(featureSubsetNValue > 0, "特征子集比例不是正数")
      if (featureSubsetNValue > 1.0) {
        return featureSubsetNValue.toInt.toString
      }
      return featureSubsetNValue.toString
    }
    return res
  }

}
