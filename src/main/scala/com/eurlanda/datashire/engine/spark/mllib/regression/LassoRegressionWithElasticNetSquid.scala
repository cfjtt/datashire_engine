package com.eurlanda.datashire.engine.spark.mllib.regression

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
  * Created by Administrator on 2017-05-24.
  *  Lasso回归
  */
class LassoRegressionWithElasticNetSquid extends Serializable {

  private val log: Log = LogFactory.getLog(classOf[LassoRegressionWithElasticNetSquid])

  var regularization = 0.0
  var maxIter = 0
  var aggregationDepth = 0
  var fitIntercept = false
  var solverIndex = -1  // solverIndex 从 1 开始
  var standardization = false
  var tolerance = 0.0
  var training_percentage = 0.0

  /**
    *
    * @param dataFrame  csn , 第一个元素是标签值，后面是特征值
    * @return
    */
    def run(dataFrame: DataFrame): (LinearRegressionModel,java.util.HashMap[String,Any])= {
      val elasticNetLinearRegression = new ElasticNetLinearRegressionSquid()
      elasticNetLinearRegression.regularization = this.regularization
      elasticNetLinearRegression.maxIter = this.maxIter
      elasticNetLinearRegression.aggregationDepth =this.aggregationDepth
      elasticNetLinearRegression.fitIntercept = this.fitIntercept
      elasticNetLinearRegression.solverIndex =this.solverIndex // solverIndex 从 1 开始
      elasticNetLinearRegression.standardization = this.standardization
      elasticNetLinearRegression.tolerance = this.tolerance
      elasticNetLinearRegression.training_percentage = this.training_percentage
      elasticNetLinearRegression.elasticNetParam = 1.0   //  1 是L1正则化，lasso回归

      elasticNetLinearRegression.run(dataFrame)
  }

}
