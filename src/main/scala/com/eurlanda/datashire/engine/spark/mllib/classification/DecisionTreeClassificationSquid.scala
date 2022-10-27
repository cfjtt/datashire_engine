package com.eurlanda.datashire.engine.spark.mllib.classification

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2017-08-11.
  */
class DecisionTreeClassificationSquid {

  private val log: Log = LogFactory.getLog(classOf[DecisionTreeClassificationSquid])

  var maxBins = -1 //  >= maxCategories
  var impurityIndex = -1
  var maxDepth = -1
  var minInfoGain = -1.0
  var minInstancesPerNode = -1
  var trainingDataPercentage = -1.0
  var maxCategories = 0 // 特征离散阈值 >=2
  var thresholdsCsn = "" //  类别阈值， 要等于类别标签的数量,全部 >=0 且最多只能有一个0

  /**
    *
    * @param dataFrame
    * @return
    */
  def run(dataFrame: DataFrame): (DecisionTreeClassificationModel,java.util.HashMap[String,Any]) = {

    require(maxCategories >= 2, "特征离散阈值应大于或等于2")
    require(maxBins >= maxCategories, "最大特征数应大于或等于特征离散阈值" + maxCategories)
    require(maxDepth >= 0 && maxDepth <= 30, "最大深度应在区间[0,30]")
    require(minInfoGain >= 0.0, "最小信息增益应大于或等于0")
    require(minInstancesPerNode >= 1, "每个节点最小实例数应大于或等于1")
    require(trainingDataPercentage > 0.0 && trainingDataPercentage <= 1.0, "训练集比例应在区间 ( 0,100 ]")

    val precessedNaDf = dataFrame.na.drop().persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val dataCount = precessedNaDf.count()
      if (dataCount == 0) {
        throw new RuntimeException("没有训练数据")
      }

      val labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(precessedNaDf)

      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(maxCategories)
        .fit(precessedNaDf)

      var trainingData: DataFrame = null
      var testData: DataFrame = null
      if (trainingDataPercentage == 1.0) {
        trainingData = precessedNaDf
        testData = precessedNaDf
      } else {
        val splitedDf = precessedNaDf.randomSplit(Array(trainingDataPercentage, 1 - trainingDataPercentage))
        trainingData = splitedDf.apply(0)
        testData = splitedDf.apply(1)
      }

      val dt = new DecisionTreeClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")
        .setImpurity(getImpurity)
        .setMaxBins(maxBins)
        .setMaxDepth(maxDepth)
        .setMinInfoGain(minInfoGain)
        .setMinInstancesPerNode(minInstancesPerNode)
        .setThresholds(util.parseClassificationThresholds(thresholdsCsn))

      val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)

      val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
      val pipelineModel = pipeline.fit(trainingData)
      val predictions = pipelineModel.transform(testData)

      val treeModel = pipelineModel.stages(2).asInstanceOf[DecisionTreeClassificationModel]

      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
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
      modelMetrics.put("f1",f1)
      modelMetrics.put("weightedPrecision",weightedPrecision)
      modelMetrics.put("weightedRecall",weightedRecall)

      (treeModel,modelMetrics)
    } catch {
      case e: Throwable => {
        val errmsg = e.getMessage
        log.error(errmsg)
        log.error(e.getStackTrace())
        if (errmsg.toLowerCase().contains("but was given by empty one")) {
          throw new RuntimeException("没有训练数据，可能原因：1)输入数据太少；2)训练集比例设置过大或过小")
        } else if (errmsg.toLowerCase().contains("nothing has been added to this summarizer")) {
          throw new RuntimeException("没有测试数据，可能原因：1)输入数据太少；2)训练集比例设置过大或过小", e)
        } else if (errmsg.contains("non-matching numClasses and thresholds.length")) {
          var firstNumberGroup = util.getFirstNumberGroup(errmsg)
          if(firstNumberGroup == null){
            firstNumberGroup = ""
          }
          throw new RuntimeException("分类阈值元素个数要等于分类数量" + firstNumberGroup, e)
        } else if (errmsg.contains("to be at least as large as the number of values in each categorical feature")) {
          throw new RuntimeException("训练样本数量和最大特征数都要大于或等于不同特征值的数量，" +
            "请调大训练集比例，或增加训练数据，或删除特征值数过多的特征", e)
        } else if (errmsg.contains("Considering remove this and other categorical")) {
          throw new RuntimeException("最大类数至少是每个类别特征数，建议增加训练样本或移除大的类别特征", e)
        } else if (errmsg.contains("Requested array size exceeds VM limit")) {
          throw new RuntimeException("数组大小超过java虚拟机限制，可能原因：属性参数设置过大", e)
        } else if (errmsg.contains("empty.maxBy")) {
          throw new RuntimeException("训练错误，可能原因：1）数据太少；2）训练集比例设置不合理，导致训练或测试数据太少;3)数据异常", e)
        } else if (errmsg.contains("requires label is non-negative")) {
          throw new RuntimeException("标签不能是负数", e)
        } else if (errmsg.contains("but requires label < numClasses")) {
          throw new RuntimeException("标签不是整数或不能转换为整数", e)
        }  else if (errmsg.contains("key not found")) {
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
      precessedNaDf.unpersist()
    }

  }

  private def getImpurity(): String = {
    // impurityIndex 从 0 开始
    val arr = Array("entropy", "gini")
    require(impurityIndex >= 0 && impurityIndex < arr.length, "计算标准应是" + arr.mkString(","))
    arr.apply(impurityIndex)
  }

}
