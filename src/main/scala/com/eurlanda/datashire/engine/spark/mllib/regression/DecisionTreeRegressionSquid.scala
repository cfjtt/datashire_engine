package com.eurlanda.datashire.engine.spark.mllib.regression

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2017-08-10.
  */
class DecisionTreeRegressionSquid extends Serializable {

  private val log: Log = LogFactory.getLog(classOf[DecisionTreeRegressionSquid])

  var impurityIndex = -1  //("variance")
  var maxBins = -1
  var maxDepth = -1 // maxDepth <= 30
  var minInfoGain = -1.0
  var minInstancesPerNode = -1
  var trainingDataPercentage = -1.0
  var maxCategories = 0  // 特征离散阈值

  /**
    *
    * @param dataFrame
    * @return
    */
  def run(dataFrame :DataFrame): (DecisionTreeRegressionModel,java.util.HashMap[String,Any]) = {

    require(maxCategories >= 2, "特征离散阈值应大于或等于2")
    require(maxBins >= maxCategories, "最大特征数应大于或等于特征离散阈值" + maxCategories)
    require(maxDepth >= 0 && maxDepth <= 30, "最大深度应在区间[0,30]")
    require(minInfoGain >= 0.0, "最小信息增益应大于或等于0")
    require(minInstancesPerNode >= 1, "每个节点最小实例数应大于或等于1")
    require(trainingDataPercentage > 0.0 && trainingDataPercentage <= 1.0, "训练集比例应在区间 ( 0,100 ]")

    if (dataFrame.rdd.isEmpty()) {
      throw new RuntimeException("没有训练数据")
    }

    val proceNaDf = dataFrame.na.drop().persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val dataCount = proceNaDf.count()
      if (dataCount == 0) {
        throw new RuntimeException("没有训练数据")
      }
      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(maxCategories)
        .fit(dataFrame)
      var trainingData: DataFrame = null
      var testData: DataFrame = null
      if (trainingDataPercentage == 1.0) {
        trainingData = proceNaDf
        testData = proceNaDf
      } else {
        val splitedDf = proceNaDf.randomSplit(Array(trainingDataPercentage, 1 - trainingDataPercentage))
        trainingData = splitedDf.apply(0)
        testData = splitedDf.apply(1)
      }
      val dt = new DecisionTreeRegressor()
        .setLabelCol("label")
        .setFeaturesCol("indexedFeatures")
        .setImpurity(getImpurity)
        .setMaxBins(maxBins)
        .setMaxDepth(maxDepth)
        .setMinInfoGain(minInfoGain)
        .setMinInstancesPerNode(minInstancesPerNode)

      val pipeline = new Pipeline()
        .setStages(Array(featureIndexer, dt))

      val pipelineModel = pipeline.fit(trainingData)
      val predictions = pipelineModel.transform(testData)

      val treeModel = pipelineModel.stages(1).asInstanceOf[DecisionTreeRegressionModel]

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
        r2 = 1.0
      }

      val modelMetrics = new java.util.HashMap[String,Any]()
      modelMetrics.put("dataCount",dataCount)
      modelMetrics.put("mse",mse)
      modelMetrics.put("rmse",rmse)
      modelMetrics.put("mae",mae)
      modelMetrics.put("r2",r2)

      (treeModel,modelMetrics)
    } catch {
      case e: Throwable => {
        val errmsg = e.getMessage
        log.error(errmsg)
        log.error(e.getStackTrace())
        if (errmsg.toLowerCase().contains("but was given by empty one")) {
          throw new RuntimeException("没有训练数据，可能原因：1)输入数据太少；2)训练集比例设置过大或过小", e)
        } else if (errmsg.toLowerCase().contains("nothing has been added to this summarizer")) {
          throw new RuntimeException("没有测试数据，可能原因：1)输入数据太少；2)训练集比例设置过大或过小", e)
        } else if (errmsg.contains("to be at least as large as the number of values in each categorical feature")) {
          throw new RuntimeException("训练样本数量和最大特征数都要大于或等于不同特征值的数量，" +
            "请调大训练集比例，或增加训练数据，或删除特征值数过多的特征", e)
        } else if (errmsg.contains("Requested array size exceeds VM limit")) {
          throw new RuntimeException("数组大小超过java虚拟机限制，可能原因：属性参数设置过大", e)
        } else if (errmsg.contains("Considering remove this and other categorical")) {
          throw new RuntimeException("最大类数至少是每个类别特征数，建议增加训练样本或移除大的类别特征", e)
        } else if (errmsg.contains("empty.maxBy")) {
          throw new RuntimeException("训练错误，可能原因：1）数据太少；2）训练集比例设置不合理，导致训练或测试数据太少;3)数据异常", e)
        } else if (errmsg.contains("requires label is non-negative")) {
          throw new RuntimeException("标签不能是负数", e)
        } else if (errmsg.contains("but requires label < numClasses")) {
          throw new RuntimeException("标签不是整数或不能转换为整数", e)
        } else if (errmsg.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源导致任务丢失，请稍后重试或在配置文件中调大配置项：" +
            "SPARK_EXECUTOR_MEMORY，SPARK_DRIVER_MEMORY", e)
        } else if (errmsg.contains("requirement failed")) {
          val regEx = "[\\u4e00-\\u9fa5]"
          val p = java.util.regex.Pattern.compile(regEx)
          if (p.matcher(errmsg).find()) {
            throw new RuntimeException(errmsg, e)
          }
          throw new RuntimeException("训练迭代错误，可能原因：总体或同一个key的数据过少或训练集比例过小或过大", e)
        }
        throw e
      }
    } finally {
      proceNaDf.unpersist()
    }
  }

 private def getImpurity():String= {
   // impurityIndex 从0开始
   val impurity = Array("variance") // 目前只支持一种方法
   require(impurityIndex >= 0 && impurityIndex < impurity.length, "计算标准应是" + impurity.mkString(","))
   impurity.apply(impurityIndex)
 }

}
