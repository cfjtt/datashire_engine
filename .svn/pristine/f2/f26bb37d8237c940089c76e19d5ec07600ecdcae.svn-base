package com.eurlanda.datashire.engine.spark.mllib.regression


import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2017-05-24.
  * 随机森林回归
  */
class RandomForestRegressionSquid  extends Serializable {

  private val log: Log = LogFactory.getLog(classOf[RandomForestRegressionSquid])

  var numberTrees = -1
  var impurityIndex = -1 //  从0开始  variance
  var featureSubsetStrategyIndex = -1  // 从0开始
  var featureSubsetNValue  = -1.0 // featureSubsetStrategy选 N 时的值
  var maxBins = -1
  var maxDepth = -1
  var minInfoGain = 0.0
  var subsamplingRate = 0.0
  var trainingDataPercentage = 0.0
  var maxCategories = 0 // 特征离散阈值

  /**
    *
    * @param dataFrame
    */
  def run(dataFrame:DataFrame): (RandomForestRegressionModel,java.util.HashMap[String,Any]) = {

    require(numberTrees >= 1, "树的数量应大于或等于 1")
    require(maxDepth >= 0 && maxDepth <= 30, "目前树最大深度是30")
    require(minInfoGain >= 0.0, "信息增益应大于或等于0")
    require(subsamplingRate > 0.0 && subsamplingRate <= 1.0, "采样率应在区间( 0,1 ]")
    require(trainingDataPercentage > 0.0 && trainingDataPercentage <= 1.0, "训练集比例应在区间( 0,100 ]")
    require(maxCategories >= 2, "特征离散阈值大于或等于2")
    require(maxBins >= maxCategories, "特征最大数应大于或等于特征离散阈值" + maxCategories)

    if (dataFrame == null || dataFrame.rdd.isEmpty()) {
      throw new RuntimeException("没有数据或数据数空")
    }
    val dataFramePeocessNa = dataFrame.na.drop().persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val dataCount = dataFramePeocessNa.count()
      if (dataCount == 0) {
        throw new RuntimeException("没有数据或数据数空")
      }

      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(maxCategories)
        .fit(dataFramePeocessNa)

      var trainData: DataFrame = null
      var testData: DataFrame = null
      if (trainingDataPercentage == 1.0) {
        trainData = dataFramePeocessNa
        testData = dataFramePeocessNa
      } else {
        val splited = dataFramePeocessNa.randomSplit(Array(trainingDataPercentage, 1 - trainingDataPercentage))
        trainData = splited.apply(0)
        testData = splited.apply(1)
      }

      val randomForestRegressor = new RandomForestRegressor()
        .setLabelCol("label")
        .setFeaturesCol("indexedFeatures")
        .setNumTrees(numberTrees)
        .setImpurity(getImpurity())
        .setFeatureSubsetStrategy(getFeatureSubsetStrategy())
        .setMaxBins(maxBins)
        .setMaxDepth(maxDepth)
        .setMinInfoGain(minInfoGain)
        .setSubsamplingRate(subsamplingRate)

      val pipeline = new Pipeline().setStages(Array(featureIndexer, randomForestRegressor))
      val pipelineModel = pipeline.fit(trainData)
      val predictions = pipelineModel.transform(testData)
      val model = pipelineModel.stages(1).asInstanceOf[RandomForestRegressionModel]

      val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("mse")
      val mse = evaluator.evaluate(predictions)

      evaluator.setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)

      evaluator.setMetricName("mae")
      val mae = evaluator.evaluate(predictions)

      evaluator.setMetricName("r2")
      var r2 = evaluator.evaluate(predictions)
      if (r2.toString.toLowerCase.contains("infinity") || r2.toString.toLowerCase.contains("nan")) {
        r2 = 1
      }

      val modelMetrics = new java.util.HashMap[String, Any]()
      modelMetrics.put("dataCount", dataCount)
      modelMetrics.put("mse", mse)
      modelMetrics.put("rmse", rmse)
      modelMetrics.put("mae", mae)
      modelMetrics.put("r2", r2)
      (model, modelMetrics)
    } catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("RandomForestRegressionSquid异常:" + errorMessage)
        log.error("RandomForestRegressionSquid异常:" + e.printStackTrace())
        if (errorMessage.contains("next on empty iterator") ||
          errorMessage.contains("empty.maxBy") ||
          errorMessage.contains("requires size of input RDD > 0, but was given by empty one") ||
          errorMessage.contains("Nothing has been added to this summarizer")) {
          throw new RuntimeException("没有训练数据或测试数据，可能原因：总体或同一个key的数据过少或训练集比例过小或过大", e)
        } else if (errorMessage.toLowerCase().contains("for input string")) {
          throw new RuntimeException("最大特征数太小", e)
        } else if (errorMessage.contains("to be at least as large as the number of values in each categorical feature")) {
          throw new RuntimeException("训练样本数量和最大特征数都要大于或等于不同特征值的数量，" +
            "请调大训练集比例，或增加训练数据，或删除特征值数过多的特征", e)
        } else if (errorMessage.contains("empty dataset")) {
          throw new RuntimeException("数据是是null或空字符串", e)
        } else if (errorMessage.contains("Requested array size exceeds VM limit")) {
          throw new RuntimeException("数组大小超过java虚拟机限制，可能原因：属性参数设置过大", e)
        } else if (errorMessage.contains("Considering remove this and other categorical")) {
          throw new RuntimeException("最大类数至少是每个类别特征数，建议增加训练样本或移除大的类别特征", e)
        } else if (errorMessage.contains("key not found")) {
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
      dataFramePeocessNa.unpersist()
    }
  }

  def getImpurity(): String = {
    val impurity = Array("variance") // 从0开始
    require(impurityIndex >= 0 && impurityIndex < impurity.length, "计算标准应是:" + impurity.mkString(","))
    impurity.apply(impurityIndex)
  }

  def getFeatureSubsetStrategy(): String = {
    val featureSubsetStrategy = Array("auto", "all", "onethird", "sqrt", "log2", "n")
    // 从0开始
    require(featureSubsetStrategyIndex >= 0 && featureSubsetStrategyIndex < featureSubsetStrategy.length,
      "特征子集策略应是:" + featureSubsetStrategy.mkString(","))
    val res = featureSubsetStrategy.apply(featureSubsetStrategyIndex)
    if (res.equalsIgnoreCase("n")) {
      require(featureSubsetNValue > 0, "特征子集比例不是正数")
      if (featureSubsetNValue > 1.0) {
        return featureSubsetNValue.toInt.toString
      }
      return featureSubsetNValue.toString
    }
    return res
  }


}
