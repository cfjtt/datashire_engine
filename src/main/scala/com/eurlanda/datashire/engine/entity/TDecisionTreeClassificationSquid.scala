package com.eurlanda.datashire.engine.entity

import java.io.IOException
import java.sql.{PreparedStatement, SQLException}
import java.util

import com.eurlanda.datashire.engine.spark.mllib.classification.DecisionTreeClassificationSquid
import com.eurlanda.datashire.engine.util.{CsnDataFrameUtil, DSUtil}
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017-08-11.
  */
class TDecisionTreeClassificationSquid extends TTrainSquid {

  private val log: Log = LogFactory.getLog(classOf[TDecisionTreeClassificationSquid])
  setType(TSquidType.DECISION_TREE_CLASSIFICATIONS_QUID)

  var impurityIndex = -1  //entropy,gini
  var maxBins = -1
  var maxDepth = -1  // maxDepth <= 30
  var minInfoGain = -1.0
  var minInstancesPerNode = -1
  var maxCategories = 0  // 特征离散阈值
  var thresholdsCsn = "" // 分类阈值

  private var outCoefficientDataCatch: DataFrame = null //系数

  override def run(jsc: JavaSparkContext): Object = {

    log.info("翻译TDecisionTreeClassificationSquid")

    if (preSquid.getOutRDD == null) {
      preSquid.runSquid(jsc)
    }
    if (outCoefficientDataCatch != null) {
      outDataFrame = outCoefficientDataCatch
      return outDataFrame
    }
    var conn: java.sql.Connection = null
    try {
      conn = getConnectionFromDS
    } catch {
      case e: Exception => {
        log.error("获取数据库连接异常", e)
        throw new RuntimeException(e)
      }
    }
    val tableName = this.getTableName
    val saveRecordSql = getSaveRecordSql(tableName)
    val modelVersion = init(conn,tableName)
    val preRDD = preSquid.getOutRDD.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val mpc = new DecisionTreeClassificationSquid()
      mpc.impurityIndex = this.impurityIndex
      mpc.maxBins = this.maxBins
      mpc.maxDepth = this.maxDepth
      mpc.trainingDataPercentage = this.getPercentage
      mpc.minInfoGain = minInfoGain
      mpc.minInstancesPerNode = minInstancesPerNode
      mpc.maxCategories = this.maxCategories
      mpc.thresholdsCsn = this.thresholdsCsn
      val outCoeffDataFrameArrs = new ArrayBuffer[DataFrame]()
      val hasCoeffSquid = hasCoefficientSquid

      if (key > 0) {
      //  val preRDD = preSquid.getOutRDD.persist(StorageLevel.MEMORY_AND_DISK)
        val keyDataCellList = preRDD.rdd.map(tmpmap => tmpmap.get(key)).distinct.collect()
        if (keyDataCellList == null || keyDataCellList.length == 0) {
          throw new RuntimeException("没有key值或没有数据")
        }
        keyDataCellList.foreach { kyDataCell =>
          val filterRDD = preRDD.rdd.filter(tmpmap => {
            if (DSUtil.isNull(kyDataCell)) {
              DSUtil.isNull(tmpmap.get(key))
            } else {
              kyDataCell.getData.equals(tmpmap.get(key).getData)
            }
          })
          val groupRDD = filterRDD.map(x => {
            val mp: util.Map[Integer, DataCell] = new util.HashMap[Integer, DataCell]()
            mp.put(inKey, x.get(inKey))
            mp
          })
          val groupKey = kyDataCell.getData.toString
          val dataFrame = CsnDataFrameUtil.csnToClassificationLabelFeature(getJobContext.getSparkSession, groupRDD)
          val mpcOut = mpc.run(dataFrame)
          saveModel(conn, tableName, saveRecordSql, groupKey, modelVersion, mpcOut._1, mpcOut._2)

          //是否设置了系数squid
          if(hasCoeffSquid) {
            val outCoefficientDataCatchtmp = TDecisionTreeRegressionSquid.getCoefficientDataCatch(getJobContext.getSparkSession,
              mpcOut._1.featureImportances, groupKey, modelVersion)
            outCoeffDataFrameArrs.append(outCoefficientDataCatchtmp)
          }
        }
      } else {
        var dataFrame :DataFrame = null
        if (preSquid.outDataFrame != null) {
          dataFrame = preSquid.outDataFrame //.persist(StorageLevel.MEMORY_AND_DISK)
        } else {
          dataFrame = CsnDataFrameUtil.csnToClassificationLabelFeature(getJobContext.getSparkSession, preRDD)
        }
        val mpcOut = mpc.run(dataFrame)
        saveModel(conn, tableName, saveRecordSql, key.toString, modelVersion, mpcOut._1, mpcOut._2)
        //是否设置了系数squid
        if(hasCoeffSquid) {
          val outCoefficientDataCatchtmp = TDecisionTreeRegressionSquid.getCoefficientDataCatch(getJobContext.getSparkSession,
            mpcOut._1.featureImportances, key + "", modelVersion)
          outCoeffDataFrameArrs.append(outCoefficientDataCatchtmp)
        }
      }
      //是否设置了系数squid
      if(hasCoeffSquid) {
        outCoefficientDataCatch = outCoeffDataFrameArrs.reduce(_.union(_))
        return outCoefficientDataCatch
      }
      return null
    } catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error(e.getStackTrace())
        log.error("TDecisionTreeRegressionSquid 异常:" + errorMessage)
        if(errorMessage.contains("The size of BLOB/TEXT data inserted in one transaction is greater than 10% of redo log size")){
          throw new RuntimeException("数据大小超过日志文件的10%，请在mysql的配置文件my.cnf中增加变量innodb_log_file_size的值", e)
        }
        throw e
      }
    } finally {
      if (conn != null) {
        conn.close()
      }
      if(preRDD != null){
        preRDD
      }
    }
  }

  /**
    * 保存记录
    */
  private def saveModel(conn: java.sql.Connection, tableName: String, saveModelSql: String, key: String, version: Int,
                      model: DecisionTreeClassificationModel, modelMetrics : java.util.HashMap[String,Any]) {
    val modelBytes = com.eurlanda.datashire.engine.util.IOUtils.readObjectToBytes(model)
    var preparedStatement : PreparedStatement= null
    try {
     preparedStatement = conn.prepareStatement(saveModelSql)
      // preparedStatement.setLong(1, 0)  // id 是主键，已经设置自增
     preparedStatement.setLong(1, modelMetrics.get("dataCount").toString.toLong)
     preparedStatement.setFloat(2, this.getPercentage)
     preparedStatement.setBytes(3, modelBytes)
     preparedStatement.setFloat(4, modelMetrics.get("precision").toString.toFloat)
     preparedStatement.setInt(5, model.numFeatures)
     preparedStatement.setInt(6, model.numClasses)
     preparedStatement.setDouble(7, modelMetrics.get("f1").toString.toDouble)
     preparedStatement.setDouble(8, modelMetrics.get("weightedPrecision").toString.toDouble)
     preparedStatement.setDouble(9, modelMetrics.get("weightedRecall").toString.toDouble)
     preparedStatement.setTimestamp(10, new java.sql.Timestamp(new java.util.Date().getTime))
     preparedStatement.setInt(11, version)
     preparedStatement.setString(12, key)
     preparedStatement.execute
      log.debug("保存模型到MySQL成功，表名：" + tableName)
    } catch {
      case e: com.mysql.jdbc.PacketTooBigException => {
        val msg = "训练模型" + modelBytes.length + "字节，太大不能写入数据表" + tableName
        log.error(msg, e)
        log.error(e.getStackTrace())
        throw new RuntimeException(msg, e)
      }
      case e: IOException => {
        log.error("训练模型序列化异常", e)
        log.error(e.getStackTrace())
        throw new RuntimeException(e)
      }
      case e: SQLException => {
        val errorMessage = e.getMessage
        log.error("保持训练模型异常" + errorMessage, e)
        log.error(e.getStackTrace())
        if (errorMessage.contains("is full")) {
          throw new RuntimeException("数据库空间不足，请删除无用表", e)
        } else if (errorMessage.contains("Increase the redo log size using innodb_log_file_size")) {
          throw new RuntimeException("数据BLOB/TEXT超过redo日志文件的10% ，请增加redo日志文件大小", e)
        }
        throw new RuntimeException("训练模型 SQL异常", e)
      }
    }finally {
      if(preparedStatement != null){
        preparedStatement.close()
      }
    }
  }

  private def getSaveRecordSql(tableName: String): String = {
    var sb: String = null
    if (tDataSource.getType eq DataBaseType.MYSQL) {
      sb = "insert into " + tableName + "( " +
        //  "id," +    //id 是主键，已经设置自增
        "total_dataset," +
        "training_percentage," +
        "model," +
        "`precision`," +
        "Num_Features," +
        "num_Classes," +
        "F1," +
        "Weighted_Precision ," +
        "weighted_Recall ," +
        "creation_date," +
        "version," +
        "`key`) " +
        "values(?,?,?,?,?,?,?,?,?,?,?,?)"
    } else {
      throw new RuntimeException("不匹配的数据库")
    }
    sb
  }


}
