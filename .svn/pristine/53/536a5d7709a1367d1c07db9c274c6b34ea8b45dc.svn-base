package com.eurlanda.datashire.engine.util

import com.eurlanda.datashire.engine.entity.DataCell
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, _}

/**
  * Created by Administrator on 2017-05-26.
  */
object CsnDataFrameUtil {

  /**
    * 把csn拆分成分类类型的label，feature，
    * 每行至少2个元素,第一个元素是label，后面是 features
    *  标签是整数,且范围在区间 [0, ... 2147483647)
    * @param session
    * @param csnRdd
    * @return  返回两列 label, features
    */
  def csnToClassificationLabelFeature(session: SparkSession, csnRdd: RDD[java.util.Map[Integer, DataCell]]): DataFrame = {
    val data = csnRdd.mapPartitions(par => {
      par.filter(row=>{
        var isValidValue = true
        for (i <- 0.until(row.keySet().size())) {
          val dt = row.get(row.keySet().toArray.apply(i)) // 只要有一个是null，就过滤掉该行
          if (dt == null) {
            isValidValue = false
          }
        }
        isValidValue
      }).map { row =>
        val rowArray = collection.mutable.ArrayBuffer[Any]()
        for (i <- 0.until(row.keySet().size())) {
          val dt = row.get(row.keySet().toArray.apply(i)).getData.toString.split(",") // 拆分csn
          if (dt.length < 2) {
            throw new RuntimeException("训练数据每行至少2个元素,第一个元素是标签,后面元素是特征，标签或特征是null")
          }
          try {
            val lab = dt.head.toInt
            if(lab < 0 || lab >= 2147483647){
              throw new RuntimeException("分类的标签应是整数且在区间 [ 0, 2147483647)")
            }
            rowArray.append(lab) //第一个元素是标签
          } catch {
            case e: Exception => throw new RuntimeException("分类的标签应是整数且在区间 [ 0, 2147483647)")
          }
          rowArray.append(dt.tail.mkString(",")) // 后面元素是特征
        }
        Row.fromSeq(rowArray)
      }
    })
    try {
      val df = session.createDataFrame(data.map(x => {
        val features = x.get(1).toString
        Tuple2(x.getInt(0),
          Vectors.dense(features.replace("[", "").replace("]", "").split(",").map(_.toDouble)))
      }))
      return df.toDF("label", "features")
    }catch {
      case e:Throwable =>{
        val exp = ExceptionUtil.translateExceptionMessage(e)
        if(exp != null){
          throw exp
        }
        throw new RuntimeException("数据转换错误，"+e.getMessage,e)
      }
    }
  }

  /**
    * 把csn拆分成回归类型的 label，feature，
    * 每行至少2个元素,第一个元素是label，后面是 features
    * @param session
    * @param csnRdd
    * @return  返回两列 label, features
    */
  def csnToRegressionLabelFeature(session: SparkSession, csnRdd: RDD[java.util.Map[Integer, DataCell]]): DataFrame = {
    val data = csnRdd.mapPartitions(par => {
      par.filter(row => {
        var isValidValue = true
        for (i <- 0.until(row.keySet().size())) {
          val dt = row.get(row.keySet().toArray.apply(i)) // 只要有一个是null，就过滤掉该行
          if (dt == null) {
            isValidValue = false
          }
        }
        isValidValue
      }).map { row =>
        val rowArray = collection.mutable.ArrayBuffer[Any]()
        for (i <- 0.until(row.keySet().size())) {
          val dt = row.get(row.keySet().toArray.apply(i)).getData.toString.split(",") // 拆分csn
          if (dt.length < 2) {
            throw new IllegalArgumentException("训练数据每行至少2个元素,第一个元素是标签,后面元素是特征，标签或特征是null")
          }
          rowArray.append(dt.head.toDouble) //第一个元素是标签
          rowArray.append(dt.tail.mkString(",")) // 后面元素是特征
        }
        Row.fromSeq(rowArray)
      }
    })
    try {
      val df = session.createDataFrame(data.map(x => {
        val features = x.get(1).toString
        Tuple2(x.getDouble(0),
          Vectors.dense(features.replace("[", "").replace("]", "").split(",").map(_.toDouble)))
      }))
      return df.toDF("label", "features")
    } catch {
      case e: Throwable => {
        val exp = ExceptionUtil.translateExceptionMessage(e)
        if(exp != null){
          throw exp
        }
        throw new RuntimeException("数据转换错误，" + e.getMessage, e)
      }
    }
  }

  /**
    * 把csn拆分成 features
    * @param session
    * @param csnRdd
    * @return  返回一列 features
    */
  def csnToFeatureVector(session: SparkSession, csnRdd: RDD[java.util.Map[Integer, DataCell]]): DataFrame = {
    val data = csnRdd.mapPartitions(par => {
      par.filter(row=>{
        var isValidValue = true
        for (i <- 0.until(row.keySet().size())) {
          val dt = row.get(row.keySet().toArray.apply(i)) // 只要有一个是null，就过滤掉该行
          if (dt == null) {
            isValidValue = false
          }
        }
        isValidValue
      }).map { row =>
        val rowArray = collection.mutable.ArrayBuffer[Any]()
        val arr = row.keySet().toArray
        for (i <- 0.until(row.keySet().size())) {
          val dt = row.get(arr.apply(i)).getData.toString
          rowArray.append(dt)
        }
        Row.fromSeq(rowArray)
      }
    })
    try {
      val vectordata = data.map(x => {
        val features = x.get(0).toString
        Tuple1(Vectors.dense(features.replace("[", "").replace("]", "").split(",", -1).map(_.toDouble)
        ))
      })
      val df = session.createDataFrame(vectordata)
      return df.toDF("features")
    } catch {
      case e: Throwable => {
        val exp = ExceptionUtil.translateExceptionMessage(e)
        if(exp != null){
          throw exp
        }
        throw new RuntimeException("数据转换错误，" + e.getMessage, e)
      }
    }
  }

  /**
    * 把csn拆分成回归类型的 label，feature，
    * 每行至少2个元素,第一个元素是label，后面是 features
    * @param session
    * @param csnRdd
    * @return  返回两列 label, features
    */
  def csnToClassificationLabeledPointRdd(session: SparkSession, csnRdd: RDD[java.util.Map[Integer, DataCell]]): RDD[LabeledPoint] = {
    val data = csnRdd.mapPartitions(par => {
      par.filter(row => {
        var isValidValue = true
        for (i <- 0.until(row.keySet().size())) {
          val dt = row.get(row.keySet().toArray.apply(i)) // 只要有一个是null，就过滤掉该行
          if (dt == null) {
            isValidValue = false
          }
        }
        isValidValue
      }).map { row =>
        val rowArray = collection.mutable.ArrayBuffer[Any]()
        for (i <- 0.until(row.keySet().size())) {
          val dt = row.get(row.keySet().toArray.apply(i)).getData.toString.split(",") // 拆分csn
          if (dt.length < 2) {
            throw new IllegalArgumentException("训练数据每行至少2个元素,第一个元素是标签,后面元素是特征，标签或特征是null")
          }
          rowArray.append(dt.head.toDouble) //第一个元素是标签
          rowArray.append(dt.tail.mkString(",")) // 后面元素是特征
        }
        Row.fromSeq(rowArray)
      }
    })
    try {
      data.map(x => {
        val features = x.get(1).toString
        val feature: org.apache.spark.mllib.linalg.Vector =
          new org.apache.spark.mllib.linalg.DenseVector(features.replace("[", "").replace("]", "").split(",").map(_.toDouble))
        LabeledPoint.apply(x.getDouble(0), feature)
      })
    } catch {
      case e: Throwable => {
        val exp = ExceptionUtil.translateExceptionMessage(e)
        if(exp != null){
          throw exp
        }
        throw new RuntimeException("数据转换错误，" + e.getMessage, e)
      }
    }
  }


}
