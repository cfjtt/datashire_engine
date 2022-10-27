package com.eurlanda.datashire.engine.entity

import java.io.IOException
import java.sql.{PreparedStatement, SQLException}
import java.util

import com.eurlanda.datashire.engine.spark.mllib.classification.MultilayerPerceptronClassificationSquid
import com.eurlanda.datashire.engine.util.{CsnDataFrameUtil, DSUtil}
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2017-05-24.
  */
class TMultilayerPerceptronClassificationSquid extends TTrainSquid {

  private val log: Log = LogFactory.getLog(classOf[TMultilayerPerceptronClassificationSquid])
  setType(TSquidType.MULTILAYER_PERCEPTRON_CLASSIFICATION_SQUID)

  var maxIter = -1
  var solverIndex = -1
  var tolerance =0.0
  var step_size = 0.0
  var hiddenLayersCsn :String = null  // 隐藏层，不包括第一层数输入层和最后一层是输出层
  var initialWeightsCsn :String = null

  override def run(jsc: JavaSparkContext): Object = {

    log.info("翻译多层感知机分类")

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
    val saveRecordSql = getSaveRecordSql(tableName)
    val modelVersion = init(conn,tableName)
    val preRDD = preSquid.getOutRDD.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val mpc = new MultilayerPerceptronClassificationSquid()
      mpc.solverIndex = this.solverIndex
      mpc.initialWeightsCsn = initialWeightsCsn
      mpc.hiddenLayersCsn = hiddenLayersCsn
      mpc.maxIter = this.maxIter
      mpc.step_size = this.step_size
      mpc.tolerance = this.tolerance
      mpc.trainingDataPercentage = this.getPercentage

      if (key > 0) {
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
          val dataFrame = CsnDataFrameUtil.csnToClassificationLabelFeature(getJobContext.getSparkSession, groupRDD)
          val mpcOut = mpc.run(dataFrame)
          saveModel(conn, tableName, saveRecordSql, kyDataCell.getData.toString, modelVersion, mpcOut._1, mpcOut._2)
        }
      } else {
        var dataFrame: DataFrame = null
        if (preSquid.outDataFrame != null) {
          dataFrame = preSquid.outDataFrame
        } else {
          dataFrame = CsnDataFrameUtil.csnToClassificationLabelFeature(getJobContext.getSparkSession, preRDD)
        }
        val mpcOut = mpc.run(dataFrame)
        saveModel(conn, tableName, saveRecordSql, key.toString, modelVersion, mpcOut._1, mpcOut._2)
      }
      return null
    } catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("TMultilayerPerceptronClassificationSquid 异常:" + errorMessage)
        log.error(e.getStackTrace())
        if(errorMessage.contains("The size of BLOB/TEXT data inserted in one transaction is greater than 10% of redo log size")){
          throw new RuntimeException("数据大小超过日志文件的10%，请在mysql的配置文件my.cnf中增加变量innodb_log_file_size的值", e)
        }
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

  /**
    * 保存记录
    */
  def saveModel(conn:java.sql.Connection, tableName:String, saveModelSql:String, key:String, version:Int,
                model: MultilayerPerceptronClassificationModel, modelMetrics :java.util.HashMap[String,Any]) {
    val modelBytes = com.eurlanda.datashire.engine.util.IOUtils.readObjectToBytes(model)
    var preparedStatement : PreparedStatement= null
    try {
      preparedStatement = conn.prepareStatement(saveModelSql)
     // preparedStatement.setLong(1, 0)  // id 是主键，已经设置自增
     preparedStatement.setLong(1,modelMetrics.get("dataCount").toString.toLong)
     preparedStatement.setFloat(2, getPercentage)
     preparedStatement.setString(3, model.uid)
     preparedStatement.setBytes(4, modelBytes)
     preparedStatement.setFloat(5, modelMetrics.get("precision").toString.toFloat)
     preparedStatement.setInt(6, model.numFeatures)
     preparedStatement.setString(7, model.layers.mkString(","))
     preparedStatement.setDouble(8, modelMetrics.get("f1").toString.toDouble)
     preparedStatement.setDouble(9, modelMetrics.get("weightedPrecision").toString.toDouble)
     preparedStatement.setDouble(10, modelMetrics.get("weightedRecall").toString.toDouble)
     preparedStatement.setTimestamp(11, new java.sql.Timestamp(new java.util.Date().getTime))
     preparedStatement.setInt(12, version)
     preparedStatement.setString(13, key)
     preparedStatement.execute
      log.debug("保存模型到MySQL成功... 表名：" + tableName)
    } catch {
      case e: com.mysql.jdbc.PacketTooBigException => {
        val msg: String = "训练模型" + modelBytes.length + "字节，太大不能写入数据表" + tableName
        log.error(msg, e)
        log.error(e.getStackTrace())
        throw new RuntimeException(msg, e)
      }
      case e: IOException => {
        log.error("训练模型序列化异常..", e)
        log.error(e.getStackTrace())
        throw new RuntimeException(e)
      }
      case e: SQLException => {
        val errorMessage = e.getMessage
        log.error("训练模型 SQL异常" + errorMessage, e)
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

  protected def getSaveRecordSql(tableName: String): String = {
    var sb: String = null
    if (tDataSource.getType eq DataBaseType.MYSQL) {
      sb = "insert into " + tableName + "( " +
      //  "id," +    //id 是主键，已经设置自增
        "total_dataset," +
        "training_percentage," +
        "uid," +
        "model," +
        "`precision`," +
        "num_features ," +
        "layers ," +
        "f1," +
        "weighted_precision ," +
        "weighted_recall ," +
        "creation_date," +
        "version," +
        "`key`) " +
        "values(?,?,?,?,?,?,?,?,?,?,?,?,?)"
    } else {
      throw new RuntimeException("不匹配的数据库")
    }
    sb
  }

}
