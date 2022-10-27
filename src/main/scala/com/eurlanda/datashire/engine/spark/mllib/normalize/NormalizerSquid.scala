package com.eurlanda.datashire.engine.spark.mllib.normalize

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.feature._
import org.apache.spark.sql._


/**
  * Created by Administrator on 2017-06-26.
  * 数据标准化
  */
class NormalizerSquid extends Serializable{

  private val log: Log = LogFactory.getLog(classOf[NormalizerSquid])

  var method : String = null // 标准化方法  Standard，MinMaxScaler，MaxAbsScaler
  var maxValue  = Double.NaN // method = MinMaxScaler时用到
  var minValue  = Double.NaN  // method= MinMaxScaler时用到

  /**
    *  把数据的每一列分别进行标准化
    * @param dataFrame
    * @param hasDataCatchSquid  是否计算标准后的数据
    * @return  第一个返回值是model，第二个返回值是标准后的数据,如果hasDataCatchSquid = true,
    */
  def run(dataFrame: DataFrame,hasDataCatchSquid:Boolean): (Long, NormalizerModel,DataFrame) = {

    if (method == null || method.trim.equals("")) {
      throw new RuntimeException("标准化方法是null或空字符串")
    }
    if (dataFrame == null || dataFrame.rdd.isEmpty()) {
      throw new RuntimeException("没有数据或数据是null")
    }
    val processNaData = dataFrame.na.drop().persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    if (processNaData == null || processNaData.rdd.isEmpty()) {
      throw new RuntimeException("没有数据或数据是null")
    }
    val dataCount = processNaData.count()
    if (dataCount == 0) {
      throw new RuntimeException("没有数据或数据是null")
    }

    val normalizerModel = new NormalizerModel()
    normalizerModel.method = this.method
    var normalizedData : DataFrame = null
    try {
      if (method.equalsIgnoreCase("Standard")) {
        val scaler = new StandardScaler()
          .setInputCol("features")
          .setOutputCol("scaledFeatures")
          .setWithStd(true)
          .setWithMean(true)
        val model: StandardScalerModel = scaler.fit(processNaData)
        if(hasDataCatchSquid) {
           normalizedData = model.transform(processNaData).select("scaledFeatures")
        }
        normalizerModel.normalizerModeObject = model
        return (dataCount, normalizerModel, normalizedData)

      } else if (method.equalsIgnoreCase("MinMaxScaler")) {
        if (minValue == Double.NaN) {
          throw new RuntimeException("没有设置最小值")
        }
        if (maxValue == Double.NaN) {
          throw new RuntimeException("没有设置最大值")
        }
        if (minValue >= maxValue) {
          throw new RuntimeException("最小值要小于最大值")
        }

        val scaler = new MinMaxScaler()
          .setInputCol("features")
          .setOutputCol("scaledFeatures")
          .setMin(minValue)
          .setMax(maxValue)
        val model = scaler.fit(processNaData)
        if(hasDataCatchSquid) {
          normalizedData = model.transform(processNaData).select("scaledFeatures")
        }
        normalizerModel.normalizerModeObject = model
        return (dataCount, normalizerModel, normalizedData)

      } else if (method.equalsIgnoreCase("MaxAbsScaler")) {
        val scaler = new MaxAbsScaler()
          .setInputCol("features")
          .setOutputCol("scaledFeatures")
        val model = scaler.fit(processNaData)
        if(hasDataCatchSquid) {
           normalizedData = model.transform(processNaData).select("scaledFeatures")
        }
        normalizerModel.normalizerModeObject = model
        return (dataCount, normalizerModel, normalizedData)

      } else {
        throw new RuntimeException("不存在标准化方法")
      }
    } catch {
      case e: Throwable => {
        val errmsg = e.getMessage
        log.error(errmsg)
        log.error(e.getStackTrace())
        if (errmsg.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源，请稍后重试或在配置文件中调大配置项：" +
            "SPARK_EXECUTOR_MEMORY，SPARK_DRIVER_MEMORY", e)
        }
        throw e
      }
    }finally {
      processNaData.unpersist()
    }

  }

}

/**
  *
  */
class NormalizerModel extends Serializable {

  var method: String = null // 标准化方法  Standard，MinMaxScaler，MaxAbsScaler
  var normalizerModeObject :Object = null // StandardScalerModel ,MinMaxScalerModel，MaxAbsScalerModel

  def predict(csn: String): String = {

    if(csn == null || csn.trim.equals("")){
      throw new RuntimeException("数据标准预测没有数据或数据是null")
    }
    if(method == null || method.trim.equals("")){
      throw new RuntimeException("数据标准化方法是null")
    }
    if(normalizerModeObject == null){
      throw new RuntimeException("数据标准化模型是null")
    }
    val values = csn.trim.split(",").map(_.toDouble)

    if (method.equalsIgnoreCase("Standard")) {
      val model = normalizerModeObject.asInstanceOf[StandardScalerModel]
      val mean = model.mean
      val std = model.std
      val trainFeaturelength = mean.toArray.length
      if (values.length != trainFeaturelength) {
        throw new RuntimeException("预测数据维数不等于训练数据维数"+trainFeaturelength)
      }
      // 减均值，除以方差
      val res = Array.fill(values.length)(0.0)
      for (i <- 0 until (values.length)) {
        var tmp = 0.0
        if (std.apply(i) != 0.0) {
          tmp = (values.apply(i) - mean.apply(i)) / (std.apply(i))
        }else{
          tmp = values.apply(i) - mean.apply(i)
        }
        res.update(i, tmp)
      }
      return res.mkString(",")
    } else if (method.equalsIgnoreCase("MinMaxScaler")) {
      val model = normalizerModeObject.asInstanceOf[MinMaxScalerModel]
      val trainFeaturelength = model.originalMin.toArray.length
      if (values.length != trainFeaturelength) {
        throw new RuntimeException("预测数据维数不等于训练数据维数"+trainFeaturelength)
      }
      val res = Array.fill(values.length)(0.0)
      for (i <- 0 until (values.length)) {
        val emin = model.originalMin.apply(i) //原数据最大值
        val emax =  model.originalMax.apply(i)  //原数据最小值
        val min = model.getMin // 指定最小值
        val max = model.getMax //指定最大值
        val esubx = emax - emin
        var eiskrip = 0.0
        if(esubx == 0.0) { // 原数据全都相等
          //  eiskrip = emax // 等于原数据
          eiskrip = (max + min) * 0.5 // 等于指定最大最小值的平均值
        }else {
          eiskrip = (values.apply(i) - emin) * (max - min) / esubx + min
        }
        res.update(i, eiskrip)
      }
      return res.mkString(",")
    } else if (method.equalsIgnoreCase("MaxAbsScaler")) {
      val model = normalizerModeObject.asInstanceOf[MaxAbsScalerModel]
      val trainFeaturelength = model.maxAbs.toArray.length
      if (values.length != trainFeaturelength) {
        throw new RuntimeException("预测数据维数不等于训练数据维数"+trainFeaturelength)
      }
      val res = Array.fill(values.length)(0.0)
      for (i <- 0 until (values.length)) {
        var max = model.maxAbs.apply(i)
        if(max == 0.0){
          max = 1.0
        }
        res.update(i, values.apply(i)/max)
      }
      return res.mkString(",")
    } else {
      throw new RuntimeException("不存在标准化方法" + method)
    }
  }

  /**
    * 逆标准化，把标准化后的数据用模型还原
    * @param csn  准化后的数据
    * @return
    */
  def inverseNormalizer(csn: String): String = {
    if(csn == null || csn.trim.equals("")){
      throw new RuntimeException("没有数据或数据是null")
    }
    val values = csn.trim.split(",").map(_.toDouble)

    if (method.equalsIgnoreCase("Standard")) {
      val model = normalizerModeObject.asInstanceOf[StandardScalerModel]
      val mean = model.mean
      val std = model.std
      val trainFeaturelength = mean.toArray.length
      if (values.length != trainFeaturelength) {
        throw new RuntimeException("预测数据维数不等于训练数据维数"+trainFeaturelength)
      }
      // 乘以方差， 加均值
      val res = Array.fill(values.length)(0.0)
      for (i <- 0 until (values.length)) {
        val tmp = values.apply(i) * (std.apply(i) ) + mean.apply(i)
        res.update(i, tmp)
      }
      return res.mkString(",")
    } else if (method.equalsIgnoreCase("MinMaxScaler")) {
      val model = normalizerModeObject.asInstanceOf[MinMaxScalerModel]
      val trainFeaturelength =model.originalMin.toArray.length
      if (values.length != trainFeaturelength ) {
        throw new RuntimeException("预测数据维数不等于训练数据维数"+trainFeaturelength)
      }
      val res = Array.fill(values.length)(0.0)
      for (i <- 0 until (values.length)) {
        val emin = model.originalMin.apply(i)
        val emax = model.originalMax.apply(i)
        val min = model.getMin
        val max = model.getMax
        val ei = (values.apply(i) - min)*(emax - emin)/(max - min) + emin
        res.update(i, ei)
      }
      return res.mkString(",")
    } else if (method.equalsIgnoreCase("MaxAbsScaler")) {
      val model = normalizerModeObject.asInstanceOf[MaxAbsScalerModel]
      val trainFeaturelength = model.maxAbs.toArray.length
      if (values.length != trainFeaturelength) {
        throw new RuntimeException("预测数据维数不等于训练数据维数"+trainFeaturelength)
      }
      val res = Array.fill(values.length)(0.0)
      for (i <- 0 until (values.length)) {
       res.update(i, values.apply(i) * model.maxAbs.apply(i))
      }
      return res.mkString(",")
    } else {
      throw new RuntimeException("不存在标准化方法" + method)
    }
  }


}

