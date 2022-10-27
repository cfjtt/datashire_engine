package com.eurlanda.datashire.engine.spark.mllib.classification


import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017-05-24.
  */
class MultilayerPerceptronClassificationSquid {

  private val log: Log = LogFactory.getLog(classOf[MultilayerPerceptronClassificationSquid])

  var trainingDataPercentage = 0.0
  var maxIter = -1
  var solverIndex = -1
  var tolerance = 0.0
  var step_size = 0.0  // 只有Solver = gd 时才可用
  var hiddenLayersCsn: String = null  // 隐藏层，不包括第一层数输入层和最后一层是输出层
  var initialWeightsCsn: String = null

  /**
    * total_dataset,training_percentage,model,precision,F1,weighted_precision,weighted_recall
    *
    * @param dataFrame
    */
  def run(dataFrame: DataFrame): ( MultilayerPerceptronClassificationModel,java.util.HashMap[String,Any]) = {

    require(trainingDataPercentage > 0.0 && trainingDataPercentage <= 1.0, "训练集比例应在区间 （0,100 ]")
    require(maxIter > 0, "迭代次数应大于0")
    require(tolerance >= 0.0, "容许误差应是大于或等于0")
    // require(hiddenLayersCsn != null && !hiddenLayersCsn.trim.equals(""), "隐藏层不能是空或null")
    val hiddenLayers = getHiddenLayers()
    if (dataFrame == null || dataFrame.rdd.isEmpty()) {
      throw new RuntimeException("没有数据或数据是空")
    }
    val dataFramePeocessNa = dataFrame.na.drop().persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    try {
      val dataCount = dataFramePeocessNa.count()
      if (dataCount == 0) {
        throw new RuntimeException("没有数据或数据是空")
      }
      //第一层是输入层的神经元数量应等于特征维数
      val numFeatures = dataFramePeocessNa.select("features").take(1).apply(0).getAs[org.apache.spark.ml.linalg.Vector]("features").size
      /* if (numFeatures != hiddenLayers.apply(0)) {
        throw new IllegalArgumentException("第一层的神经元数量应等于特征维数" + numFeatures)
      }*/
      //输出层，最后一层，标签不能是负数，可以不从0开始，可以间断，最后一层的神经元数量 >= 最大标签+1
      val labelMaxValue = dataFramePeocessNa.agg(Map("label" -> "max")).take(1).apply(0).getInt(0)
      /* if (hiddenLayers.last < labelMaxValue + 1) {
         throw new IllegalArgumentException("输出层的神经元数量应大于或等于" + (labelMaxValue + 1))
       }*/
      val layers = new ArrayBuffer[Int]()
      layers.append(numFeatures) //输入层
      if (hiddenLayers != null) {
        layers.appendAll(hiddenLayers) //隐藏层
      }
      layers.append(labelMaxValue + 1) //输出层

      var trainData: DataFrame = null
      var testData: DataFrame = null
      if (trainingDataPercentage == 1.0) {
        trainData = dataFramePeocessNa
        testData = dataFramePeocessNa
      } else {
        val Array(trainingDatatmp, testDatatmp) = dataFramePeocessNa.randomSplit(Array(trainingDataPercentage, 1 - trainingDataPercentage))
        trainData = trainingDatatmp
        testData = testDatatmp
      }

      val multilayerPerceptronClassifier = new MultilayerPerceptronClassifier()
        .setLayers(layers.toArray)
        .setMaxIter(maxIter)
        .setTol(tolerance)

      if (initialWeightsCsn != null && !initialWeightsCsn.trim().equals("")) {
        val initialWeightsCount = getInitialWeightsCount(layers.toArray)
        val initialWeights = getTrimInitialWeights(initialWeightsCount)
        multilayerPerceptronClassifier.setInitialWeights(initialWeights)
      }

      val solve = getSolver()
      multilayerPerceptronClassifier.setSolver(solve)
      if (solve.equalsIgnoreCase("gd")) {
        require(tolerance >= 0.0 && tolerance <= 1.0, "求解方法是gd时容许误差应在区间[0,1]")
        multilayerPerceptronClassifier.setTol(tolerance)

        require(step_size > 0.0, "step_size应大于0") //只有Solver = gd 时才有step_size
        multilayerPerceptronClassifier.setStepSize(step_size)
      }
      val model = multilayerPerceptronClassifier.fit(trainData)

      val testResult = model.transform(testData)
      val predictionAndLabels = testResult.select("prediction", "label")

      val evaluator = new MulticlassClassificationEvaluator()
      var precision = evaluator.setMetricName("accuracy").evaluate(predictionAndLabels)
      if (precision.toString.toLowerCase.contains("infinity") || precision.toString.toLowerCase.contains("nan")) {
        precision = 1.0
      }

      val f1 = evaluator.setMetricName("f1").evaluate(predictionAndLabels)
      val weightedPrecision = evaluator.setMetricName("weightedPrecision").evaluate(predictionAndLabels)
      val weightedRecall = evaluator.setMetricName("weightedRecall").evaluate(predictionAndLabels)

      val modelMetrics = new java.util.HashMap[String,Any]()
      modelMetrics.put("dataCount",dataCount)
      modelMetrics.put("precision",precision)
      modelMetrics.put("f1",f1)
      modelMetrics.put("weightedPrecision",weightedPrecision)
      modelMetrics.put("weightedRecall",weightedRecall)
      (model,modelMetrics)
    }
    catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("MultilayerPerceptronClassification异常:" + errorMessage)
        log.error("MultilayerPerceptronClassification异常:" + e.printStackTrace())
        if (errorMessage.contains("next on empty iterator") ||
          errorMessage.contains("Nothing has been added to this summarizer")) {
          throw new RuntimeException("没有训练数据或测试数据，可能原因：总体或同一个key的数据过少或训练集比例过小或过大", e)
        } else if (errorMessage.contains("Classifier found max label value")) {
          throw new IllegalArgumentException("分类数据的标签不在整数区间[0, 2147483647),或是负数、浮点数", e)
        } else if (errorMessage.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源导致任务丢失，请稍后重试或在配置文件中调大配置项：" +
            "SPARK_EXECUTOR_MEMORY，SPARK_DRIVER_MEMORY", e)
        }
        throw e
      }
    } finally {
      dataFramePeocessNa.unpersist()
    }
  }

  /**
    * 权值的数量，包括权值和偏差
    * (layers[i]+1)*layers[i+1]
    *
    * @param numNeuronOfLayers 每层神经元的数量
    * @return
    */
  def getInitialWeightsCount(numNeuronOfLayers: Array[Int]): Int = {
    var res = 0
    for (i <- 0.until(numNeuronOfLayers.length - 1)) {
      res += (numNeuronOfLayers.apply(i) + 1) * numNeuronOfLayers.apply(i + 1)
    }
    res
  }

  /**
    *
    * @return
    */
  private def getSolver(): String = {
    // solverIndex从1 开始
    val solvers = Array("gd", "l-bfgs")
    require(solverIndex >= 1 && solverIndex <= solvers.length, "求解方法应是:" + solvers.mkString(","))
    solvers.apply(solverIndex - 1)
  }

  /**
    * 初始权重,不够则补0, 多了则截取
    * @param initialWeightsCount
    * @return
    */
  def getTrimInitialWeights(initialWeightsCount: Int): DenseVector = {
    val res = new ArrayBuffer[Double]()
    var intweights = initialWeightsCsn.trim.replaceAll("'","").split(",", -1).map(x => {
      if (x == null || x.trim.equals("")) 0.0 else x.trim.toDouble
    })
    if (intweights.size < initialWeightsCount) { //不够，则补0
      res.appendAll(intweights)
      val sss = Vectors.zeros(initialWeightsCount - intweights.size)
      res.appendAll(sss.toArray)
    } else if (intweights.size > initialWeightsCount) {// 多了，则截取
      intweights = intweights.slice(0, initialWeightsCount)
      res.appendAll(intweights)
    }else{
      res.appendAll(intweights)
    }
    new org.apache.spark.ml.linalg.DenseVector(res.toArray)
  }

  /***
    * 每个隐藏层神经元数量
    * 不包括第一层数输入层和最后一层数输出层
    * @return
    */
  def getHiddenLayers(): Array[Int] ={
    if(hiddenLayersCsn == null || hiddenLayersCsn.trim.equals("")){
      return null
    }
    val layers = hiddenLayersCsn.replaceAll("'","").split(",").map(_.toInt)
    return layers
  }

}
