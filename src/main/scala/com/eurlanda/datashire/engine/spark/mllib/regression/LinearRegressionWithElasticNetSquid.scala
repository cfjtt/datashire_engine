package com.eurlanda.datashire.engine.spark.mllib.regression

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql._

/**
  * Created by Administrator on 2017-08-10.
  */
class LinearRegressionWithElasticNetSquid {

  private val log: Log = LogFactory.getLog(classOf[LinearRegressionWithElasticNetSquid])

  var regularization = -1.0
  var maxIter = -1
  var fitIntercept = false
  var solverIndex = -1  // solverIndex 从 1 开始
  var standardization = false
  var training_percentage = -1.0
  var elasticNetParam = -1.0
  var tolerance = -1.0

  def run(dataFrame: DataFrame): (LinearRegressionModel,java.util.HashMap[String,Any])= {
    val elasticNetLinearRegression = new ElasticNetLinearRegressionSquid()
    elasticNetLinearRegression.regularization = this.regularization
    elasticNetLinearRegression.maxIter = this.maxIter
    elasticNetLinearRegression.aggregationDepth = 2
    elasticNetLinearRegression.fitIntercept = this.fitIntercept
    elasticNetLinearRegression.solverIndex =this.solverIndex // solverIndex 从 1 开始
    elasticNetLinearRegression.standardization = this.standardization
    elasticNetLinearRegression.tolerance = this.tolerance
    elasticNetLinearRegression.training_percentage = this.training_percentage
    elasticNetLinearRegression.elasticNetParam = elasticNetParam   //  1 是L1正则化，lasso回归

    elasticNetLinearRegression.run(dataFrame)
  }


}
