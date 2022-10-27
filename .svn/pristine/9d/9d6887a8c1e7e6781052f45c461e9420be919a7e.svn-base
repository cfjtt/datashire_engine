package com.eurlanda.datashire.engine.entity

import java.io.IOException
import java.sql.{PreparedStatement, SQLException, Timestamp}
import java.util

import com.eurlanda.datashire.engine.spark.mllib.regression.RandomForestRegressionSquid
import com.eurlanda.datashire.engine.util.{CsnDataFrameUtil, DSUtil}
import com.eurlanda.datashire.enumeration.DataBaseType
import com.mysql.jdbc.PacketTooBigException
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2017-05-24.
  */
class TRandomForestRegressionSquid extends TTrainSquid {

  private val log: Log = LogFactory.getLog(classOf[TRandomForestRegressionSquid])
  setType(TSquidType.RANDOMFOREST_REGRESSION_SQUID)

  var numberTrees = -1
  var impurityIndex = -1 // "variance" // "gini"
  var featureSubsetStrategyIndex = -1   //auto,all,onethird,sqrt,log2,n
  var featureSubsetNValue = -1.0  // featureSubsetStrategy选 N 时的值
  var maxBins = -1
  var maxDepth = -1
  var minInfoGain = 0.0
  var subsamplingRate = 0.0
  var maxCategories = 0

  override def run(jsc: JavaSparkContext): Object = {

    log.info("翻译随机森林回归")

    if (preSquid.getOutRDD == null) {
      preSquid.runSquid(jsc)
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
    val saveModelSql = getSaveModelSql(tableName)
    val modelVersion = init(conn,tableName)
    val preRDD = preSquid.getOutRDD.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val randomForestReg = new RandomForestRegressionSquid()
      randomForestReg.featureSubsetStrategyIndex = this.featureSubsetStrategyIndex
      randomForestReg.featureSubsetNValue = this.featureSubsetNValue
      randomForestReg.impurityIndex = this.impurityIndex
      randomForestReg.maxBins = this.maxBins
      randomForestReg.maxDepth = this.maxDepth
      randomForestReg.minInfoGain = this.minInfoGain
      randomForestReg.numberTrees = this.numberTrees
      randomForestReg.subsamplingRate = this.subsamplingRate
      randomForestReg.trainingDataPercentage = this.getPercentage
      randomForestReg.maxCategories = this.maxCategories

      if (key > 0) {
        val keyDataCellList = preRDD.rdd.map(tmpmap => tmpmap.get(key)).distinct.collect()
        if(keyDataCellList == null || keyDataCellList.length==0){
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
          val groupRDD = filterRDD.map(x=> {
            val mp: util.Map[Integer, DataCell] = new util.HashMap[Integer, DataCell]()
            mp.put(inKey, x.get(inKey))
            mp
          })
          val dataFrame = CsnDataFrameUtil.csnToRegressionLabelFeature(getJobContext.getSparkSession, groupRDD)
          val randomForestRegOut = randomForestReg.run(dataFrame)
          saveModel(conn,tableName,saveModelSql,kyDataCell.getData.toString,modelVersion, randomForestRegOut._1, randomForestRegOut._2)
        }
      } else {
        var dataFrame: DataFrame = null
        if (preSquid.outDataFrame != null) {
          dataFrame = preSquid.outDataFrame
        } else {
          dataFrame = CsnDataFrameUtil.csnToRegressionLabelFeature(getJobContext.getSparkSession, preRDD)
        }
        val randomForestRegOut = randomForestReg.run(dataFrame)

        saveModel(conn, tableName, saveModelSql, key.toString, modelVersion, randomForestRegOut._1, randomForestRegOut._2)
      }

      return null
    } catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("TRandomForestRegressionSquid异常:" +errorMessage)
        log.error(e.getStackTrace())
        throw e
      }
    }finally {
      if(conn != null){
        conn.close()
      }
      if(preRDD != null){
        preRDD.unpersist()
      }
    }

  }

  protected override def getSaveModelSql(tableName: String): String = {
    var sb: String = null
    if (tDataSource.getType eq DataBaseType.MYSQL) {
      sb = "insert into " + tableName + "( " +
     //   "id ," +   // id 是主键，已经设置自增
        "total_dataset ," +
        "training_percentage ," +
        "uid ," +
        "model ," +
        "`PRECISION`," +
        "trees ," +
        "feature_importances,"+
        "num_features ," +
        "tree_weights ,"+
        "MSE ," +
        "RMSE ," +
        "R2 ," +
        "MAE ," +
        "creation_date," +
        "VERSION," +
        "`key`" + ") " +
        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    } else {
      throw new RuntimeException("不匹配的数据库")
    }
    sb
  }

  /**
    * 保存记录
    *
    */
  def saveModel(conn:java.sql.Connection, tableName:String, saveModelSql:String, key:String, modelVersion:Int,
               model: RandomForestRegressionModel, modelMetrics : java.util.HashMap[String,Any]) {

    // 判断表是否存在，不存在创建,模型表由用户创建
    val modelBytes = com.eurlanda.datashire.engine.util.IOUtils.readObjectToBytes(model)
    var preparedStatement: PreparedStatement = null
    try {
      preparedStatement = conn.prepareStatement(saveModelSql)
   //   preparedStatement.setLong(1, 0)  // id 是主键，已经设置自增
      preparedStatement.setLong(1, modelMetrics.get("dataCount").toString.toLong)
      preparedStatement.setFloat(2, getPercentage)
      preparedStatement.setString(3, model.uid)
      preparedStatement.setBytes(4, modelBytes)
      preparedStatement.setDouble(5,1.0)
      preparedStatement.setString(6, model.trees.mkString(","))
      preparedStatement.setString(7, model.featureImportances.toArray.mkString(","))
      preparedStatement.setInt(8, model.numFeatures)
      preparedStatement.setString(9, model.treeWeights.mkString(",")  )
      preparedStatement.setDouble(10, modelMetrics.get("mse").toString.toDouble)
      preparedStatement.setDouble(11, modelMetrics.get("rmse").toString.toDouble)
      preparedStatement.setDouble(12, modelMetrics.get("r2").toString.toDouble)
      preparedStatement.setDouble(13,modelMetrics.get("mae").toString.toDouble)
      preparedStatement.setTimestamp(14, new Timestamp(new java.util.Date().getTime))
      preparedStatement.setInt(15, modelVersion)
      preparedStatement.setString(16, key)
      preparedStatement.execute
      log.debug("保存模型到MySQL成功, 表名：" + tableName)
    } catch {
      case e: PacketTooBigException => {
        val msg = "训练模型" + modelBytes.length + "字节，太大不能写入数据表" + tableName
        log.error(msg, e)
        log.error(e.getStackTrace())
        throw new RuntimeException(msg, e)
      }
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("保存训练模型异常" + errorMessage, e)
        log.error(e.getStackTrace())
        if (errorMessage.contains("is full")) {
          throw new RuntimeException("数据库空间不足", e)
        }else if(errorMessage.contains("The size of BLOB/TEXT data inserted in one transaction is greater than 10% of redo log size")){
          throw new RuntimeException("数据大小超过日志文件的10%，请在mysql的配置文件my.cnf中增加变量innodb_log_file_size的值", e)
        }
        throw e
      }
    }finally {
      if(preparedStatement != null){
        preparedStatement.close()
      }
    }
  }


}
