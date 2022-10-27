package com.eurlanda.datashire.engine.spark.mllib.regression

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2017-07-14.
  * 弹性网回归是lasso回归（L2）和岭回归(L1) 的加权组合，elasticNetParam  取1 时是lasso回归 ，取0 时是岭回归
  *
  */
class ElasticNetLinearRegressionSquid extends Serializable {

  private val log: Log = LogFactory.getLog(classOf[ElasticNetLinearRegressionSquid])

  var regularization = 0.0
  var maxIter = 0
  var aggregationDepth = 0
  var fitIntercept = false
  var solverIndex = -1  // solverIndex 从 1 开始
  var standardization = false
  var tolerance = 0.0
  var training_percentage = 0.0
  var elasticNetParam = 0.0 // 取1 时是lasso回归 ，取0 时是岭回归

  /**
    *
    * @param dataFrame  csn , 第一个元素是标签值，后面是特征值
    * @return
    */
    def run(dataFrame: DataFrame): (LinearRegressionModel,java.util.HashMap[String,Any])= {
    require(regularization >= 0.0, "regularization应大于或等于0")
    require(maxIter >= 0, "maxIter应大于或等于0")
    require(aggregationDepth > 0, "aggregationDepth应大于或等于1")
    require(tolerance >= 0.0, "容许误差应大于或等于0")
    require(training_percentage >= 0.0 && training_percentage <= 1.0, "training_percentage应在区间( 0, 1 ]")
    require(elasticNetParam >= 0.0 && elasticNetParam <= 1.0, "弹性网参数应在区间 [0,1]")
    if (dataFrame == null || dataFrame.rdd.isEmpty()) {
      throw new RuntimeException("没有数据或数据是空")
    }
    val dataFrameProcessNa = dataFrame.na.drop().persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val dataCount = dataFrameProcessNa.count()
      if (dataCount == 0) {
        throw new RuntimeException("没有数据或数据是空")
      }
      var trainData: DataFrame = null
      var testData: DataFrame = null
      if (training_percentage == 1.0) {
        trainData = dataFrameProcessNa
        testData = dataFrameProcessNa
      } else {
        val Array(trainingDatatmp, testDatatmp) = dataFrameProcessNa.randomSplit(Array(training_percentage, 1 - training_percentage))
        trainData = trainingDatatmp
        testData = testDatatmp
      }

      val linearRegression = new LinearRegression()
        .setMaxIter(maxIter)
        .setRegParam(regularization)
        .setElasticNetParam(elasticNetParam) // 1 是L1正则化，lasso回归
        .setTol(tolerance)
        .setStandardization(standardization)
        .setSolver(getSolver()) //auto", "l-bfgs", "normal"
        .setFitIntercept(fitIntercept)
        .setAggregationDepth(aggregationDepth)

      val model = linearRegression.fit(trainData)

      var mse = model.summary.meanSquaredError
      if (mse.toString.equalsIgnoreCase("nan")) {
        mse = Double.MaxValue
      }

      var rmse = model.summary.rootMeanSquaredError
      if (rmse.toString.equalsIgnoreCase("nan")) {
        rmse = Double.MaxValue
      }

      var mae = model.summary.meanAbsoluteError
      if (mae.toString.equalsIgnoreCase("nan")) {
        mae = Double.MaxValue
      }

      var r2 = model.summary.r2
      if (r2.toString.toLowerCase.contains("infinity") || r2.toString.toLowerCase.contains("nan")) {
        r2 = 1
      }

      var explainedVariance = model.summary.explainedVariance
      if(explainedVariance.toString.equalsIgnoreCase("nan")){
        explainedVariance = Double.MaxValue
      }

      var devianceResiduals = ""
      if(model.summary.devianceResiduals != null){
        devianceResiduals = model.summary.devianceResiduals.mkString(",")
      }

     val modelMetrics = new java.util.HashMap[String,Any]()
      modelMetrics.put("dataCount",dataCount)
      modelMetrics.put("mse",mse)
      modelMetrics.put("rmse",rmse)
      modelMetrics.put("mae",mae)
      modelMetrics.put("r2",r2)
      modelMetrics.put("explainedVariance",explainedVariance)
      modelMetrics.put("devianceResiduals",devianceResiduals)

      (model,modelMetrics)
    } catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("LassoRegressionWithElasticNetSquid 异常:" + errorMessage)
        log.error("LassoRegressionWithElasticNetSquid异常:" + e.getStackTrace())
        if (errorMessage.contains("next on empty iterator") ||
          errorMessage.toLowerCase.contains("nothing has been added to this summarizer")) {
          throw new RuntimeException("没有训练数据或测试数据，可能原因：总体或同一个key的数据过少或训练集比例过小或过大", e)
        } else if (errorMessage.toLowerCase().contains("but was given by empty one")) {
          throw new RuntimeException("没有训练数据，可能原因：1)输入数据太少；2)训练集比例设置过大或过小", e)
        } else if (errorMessage.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源导致任务丢失，请稍后重试或在配置文件中调大配置项：" +
            "SPARK_EXECUTOR_MEMORY，SPARK_DRIVER_MEMORY", e)
        } else if(errorMessage.contains("The standard deviation of the label is zero. Model cannot be regularized with standardization=true")){
          throw new RuntimeException("标签全都相同，标准化数据且不拟合截距时正则化参数要是0", e)
        } else if (errorMessage.contains("requirement failed")) {
          throw new RuntimeException("没有训练数据或测试数据，可能原因：总体或同一个key的数据过少或训练集比例过小或过大", e)
        }
        throw e
      }
    } finally {
      dataFrameProcessNa.unpersist()
    }

  }

  /**
    *
    * auto, l-bfgs, normal
    * @return
    */
  def getSolver(): String = {
    // solverIndex 从1开始
    val solvers = Array("auto", "l-bfgs", "normal")
    require(solverIndex >= 1 && solverIndex <= solvers.length, "计算方法应是" + solvers.mkString(","))
    solvers.apply(solverIndex - 1)
  }

}

