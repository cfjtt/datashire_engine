package com.eurlanda.datashire.engine.entity

import java.io.IOException
import java.sql.{PreparedStatement, SQLException}
import java.util

import com.eurlanda.datashire.engine.spark.mllib.cluster.BisectingKMeansSquid
import com.eurlanda.datashire.engine.util.{CsnDataFrameUtil, DSUtil}
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.ml.clustering.BisectingKMeansModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

class TBisectingKMeansSquid extends TTrainSquid {

  private val log: Log = LogFactory.getLog(classOf[TBisectingKMeansSquid])
  setType(TSquidType.BISECTING_K_MEANS_SQUID)

  var trainDataPercentage = 1.0
  var minDivisibleClusterSize = 1.0
  var maxIterations = 10
  var k = 4

  override def run(jsc: JavaSparkContext): Object = {

    log.debug("翻译TBisectingKMeansSquid")

    if (preSquid.getOutRDD == null) {
      preSquid.runSquid(jsc)
    }

    var connection: java.sql.Connection = null
    try {
      connection = getConnectionFromDS
    } catch {
      case e: Exception => {
        log.error("获取数据库连接异常", e)
        throw new RuntimeException(e)
      }
    }
    val tableName = this.getTableName
    val saveModelSql = getSaveModelSql(tableName)
    val modelVersion = init(connection,tableName)
    val preRDD = preSquid.getOutRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val bisectingKMean = new BisectingKMeansSquid()
    bisectingKMean.trainDataPercentage = this.trainDataPercentage
    bisectingKMean.minDivisibleClusterSize = this.minDivisibleClusterSize
    bisectingKMean.maxIterations = this.maxIterations
    bisectingKMean.k = this.k
    try {
      if (key > 0) {
        var keyDataCellList = preRDD.rdd.map(tmpmap => tmpmap.get(key)).distinct.collect().toList
        if (keyDataCellList == null || keyDataCellList.length == 0) {
          throw new RuntimeException("没有key值或没有数据")
        }
        val keyDataCellQueue: util.Queue[DataCell] = new util.LinkedList[DataCell]()
        keyDataCellList.foreach(x=> keyDataCellQueue.add(x))
        keyDataCellList = null
        while (keyDataCellQueue.size()>0) {
          val kyDataCell = keyDataCellQueue.poll()
          // 包含一个key的所有数据
          val filterRDD = preRDD.rdd.filter(tmpmap => {
            if (DSUtil.isNull(kyDataCell)) {
              DSUtil.isNull(tmpmap.get(key))
            } else {
              kyDataCell.getData.equals(tmpmap.get(key).getData)
            }
          })
        //  preRDD = preRDD.subtract(filterRDD).persist(StorageLevel.MEMORY_AND_DISK) // 移除已经计算过的数据
          val groupRDD = filterRDD.map(x => {
            val mp: util.Map[Integer, DataCell] = new util.HashMap[Integer, DataCell]()
            mp.put(inKey, x.get(inKey))
            mp
          })
          val groupKey = kyDataCell.getData.toString
          val dataFrame = CsnDataFrameUtil.csnToFeatureVector(getJobContext.getSparkSession, groupRDD)
          val trainModel = bisectingKMean.run(dataFrame)
          saveModel(connection, tableName, saveModelSql, groupKey, modelVersion, trainModel._1, trainModel._2)
        }
      } else {
        var dataFrame: DataFrame = null
        if (preSquid.outDataFrame != null) {
          dataFrame = preSquid.outDataFrame
        } else {
          dataFrame = CsnDataFrameUtil.csnToFeatureVector(getJobContext.getSparkSession, preRDD)
        }
        val trainModel = bisectingKMean.run(dataFrame)
        saveModel(connection, tableName, saveModelSql, key + "", modelVersion, trainModel._1, trainModel._2)
      }
      return null
    } catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error(e.getStackTrace())
        log.error("TBisectingKMeansSquid 异常:" + errorMessage)
        if (errorMessage.contains("The size of BLOB/TEXT data inserted in one transaction is greater than 10% of redo log size")) {
          throw new RuntimeException("数据大小超过日志文件的10%，请在mysql的配置文件my.cnf中增加变量innodb_log_file_size的值", e)
        } else if (errorMessage.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源导致任务丢失，请稍后重试或调大K值或调大最小样本数", e)
        } else if(errorMessage.contains("Container killed by YARN for exceeding memory limits")){
          throw new RuntimeException("超过节点executor内存限制,请调大参数")
        }
        throw e
      }
    } finally {
      if (connection != null) {
        connection.close()
      }
      if (preRDD != null) {
        preRDD
      }
    }
  }

  /**
    * 保存记录
    */
  private def saveModel(conn: java.sql.Connection, tableName: String, saveModelSql: String, groupKey: String,
                        modelVersion: Int, model: BisectingKMeansModel, modelMetrics: java.util.HashMap[String, Any]) {
    val modelBytes = com.eurlanda.datashire.engine.util.IOUtils.readObjectToBytes(model)
    var preparedStatement: PreparedStatement = null
    try {
      preparedStatement = conn.prepareStatement(saveModelSql)
      // preparedStatement.setLong(1, 0)  // id 是主键，已经设置自增
      preparedStatement.setLong(1, modelMetrics.get("dataCount").toString.toLong)
      preparedStatement.setFloat(2, this.getPercentage)
      preparedStatement.setBytes(3, modelBytes)
      preparedStatement.setInt(4, model.summary.k)
      preparedStatement.setDouble(5, modelMetrics.get("SSE").toString.toDouble)
      preparedStatement.setString(6, model.summary.clusterSizes.mkString(","))
      preparedStatement.setTimestamp(7, new java.sql.Timestamp(new java.util.Date().getTime))
      preparedStatement.setInt(8, modelVersion)
      preparedStatement.setString(9, groupKey)
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
      case e:java.lang.OutOfMemoryError =>{
        val errorMessage = e.getMessage
        log.error("保存训练模型异常" + errorMessage, e)
        if(errorMessage.contains("Requested array size exceeds VM limit")){
          throw new RuntimeException("保存训练模型异常:可能原因：参数太大，请调小参数", e)
        }
        throw new RuntimeException("保存训练模型异常", e)
      }
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("保存训练模型异常" + errorMessage, e)
        log.error(e.getStackTrace())
        if (errorMessage.contains("is full")) {
          throw new RuntimeException("数据库空间不足，请删除无用表", e)
        } else if (errorMessage.contains("Increase the redo log size using innodb_log_file_size")) {
          throw new RuntimeException("数据BLOB/TEXT超过redo日志文件的10% ，请增加redo日志文件大小", e)
        } else if(errorMessage.contains("Java heap space")){
          throw new RuntimeException("保存训练模型异常，可能原因：参数太大导致内存不足", e)
        } else if (errorMessage.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源导致任务丢失，请稍后重试或调小参数", e)
        }
        throw new RuntimeException("保存训练模型异常", e)
      }
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close()
      }
    }
  }

  protected override def getSaveModelSql(tableName: String): String = {
    val sb = new StringBuilder()
    if (tDataSource.getType.equals(DataBaseType.MYSQL)) {
      sb.append("insert into ")
      sb.append(tableName)
      sb.append(" ( ")
      //   sb.append( "id ," ) //id 是主键，已经设置自增
      sb.append(" total_dataset ,")
      sb.append(" training_percentage ,")
      sb.append(" model ,")
      sb.append(" k ,")
      sb.append(" SSE ,")
      sb.append(" Cluster_Sizes ,")
      sb.append(" creation_date ,")
      sb.append(" version ,")
      sb.append(" `key` ")
      sb.append(" ) ")
      sb.append(" values(?,?,?,?,?,?,?,?,?)")
    } else {
      throw new RuntimeException("不匹配的数据库")
    }
    sb.toString()
  }


}
