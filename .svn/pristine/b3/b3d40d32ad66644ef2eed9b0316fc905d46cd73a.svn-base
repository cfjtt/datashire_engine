package com.eurlanda.datashire.engine.entity

import java.sql.{PreparedStatement, Timestamp}
import java.util

import com.eurlanda.datashire.engine.spark.mllib.normalize.{NormalizerModel, NormalizerSquid}
import com.eurlanda.datashire.engine.util.{CsnDataFrameUtil, DSUtil}
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.commons.logging.LogFactory
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017-06-26.
  * 翻译数据标准化
  */
class TNormalizerSquid extends TTrainSquid  {

  private val log = LogFactory.getLog(classOf[TNormalizerSquid])
  setType(TSquidType.NORMALIZER_SQUID)

  var methodIndex = -1
  var maxValue = Double.NaN
  var minValue = Double.NaN
  private var outDataCatch: DataFrame = null

  override def run(jsc: JavaSparkContext): Object = {

    if (outDataCatch != null) {
      this.outDataFrame = outDataCatch
      this.outDataFrame
    }

    log.info("翻译 NORMALIZER_SQUID")

    if (preSquid.getOutRDD == null) {
      preSquid.runSquid(jsc)
    }

    if (methodIndex == -1) {
      throw new RuntimeException("没有选标准化方法")
    }
    val normalizer = new NormalizerSquid()
    normalizer.method = getMethod()
    normalizer.minValue = this.minValue
    normalizer.maxValue = this.maxValue

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
    val dataFrameArrs = new ArrayBuffer[DataFrame]()
    val hasDatacatchSquidtmp = hasDataCatchSquid
    val preRDD = preSquid.getOutRDD.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      if (key > 0) {
        val keyDataCellList = preRDD.rdd.map(tmpmap => tmpmap.get(key)).distinct.collect()
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
          val dataFrame = CsnDataFrameUtil.csnToFeatureVector(getJobContext.getSparkSession, groupRDD)
          val res = normalizer.run(dataFrame,hasDatacatchSquidtmp)
          saveModel(conn, tableName, saveModelSql, kyDataCell.getData.toString, modelVersion, res._1, res._2)
          if (hasDatacatchSquidtmp) {  //标准化后的结果
            dataFrameArrs.append(res._3)
          }
        }
        if (hasDatacatchSquidtmp) {
          outDataCatch = dataFrameArrs.reduce(_.union(_)) // 标准化后的结果
          this.outDataFrame = outDataCatch
          return outDataFrame
        }else{
          return null
        }
      } else {
        var dataFrame: DataFrame = null
        if (preSquid.outDataFrame != null) {
          dataFrame = preSquid.outDataFrame
        } else {
          dataFrame = CsnDataFrameUtil.csnToFeatureVector(getJobContext.getSparkSession, preRDD)
        }
        val res = normalizer.run(dataFrame,hasDatacatchSquidtmp)
        saveModel(conn, tableName, saveModelSql, key.toString, modelVersion, res._1, res._2)
        if (hasDatacatchSquidtmp) {  //标准化后的结果
          outDataCatch = res._3
          this.outDataFrame = outDataCatch
          return outDataCatch
        }else{
          return null
        }
      }
    } catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error(e.getStackTrace())
        if (errorMessage.contains("empty String")) {
          throw new RuntimeException("数据有null或空值")
        } else if (errorMessage.contains("The size of BLOB/TEXT data inserted in one transaction is greater than 10% of redo log size")) {
          throw new RuntimeException("数据大小超过日志文件的10%，请在mysql的配置文件my.cnf中增加变量innodb_log_file_size的值", e)
        }
        log.error("TNormalizerSquid异常:" + errorMessage)
        throw e
      }
    } finally {
      if (conn != null) {
        conn.close()
      }
      if(preRDD != null){
        preRDD.unpersist()
      }
    }
  }

  private def getMethod(): String ={
    val mrthods = Array("Standard","MinMaxScaler","MaxAbsScaler")
    if(methodIndex<0 || methodIndex>= mrthods.length ){
      throw new RuntimeException("标准化方法选择错误，应是"+mrthods.mkString(","))
    }
    mrthods.apply(methodIndex)
  }

  protected override def getSaveModelSql(tableName:String): String = {
    var sb: String = null
    if (tDataSource.getType eq DataBaseType.MYSQL) {
      sb = "insert into " + tableName + "( " +
     //   "id," + //  id 是主键，已经设置自增
        "total_dataset," +
        "model," +
        "creation_date ," +
        "version," +
        "`key`" +
        ") values(?,?,?,?,?)"
    } else {
      throw new RuntimeException("不匹配的数据库")
    }
    sb
  }

  /**
    * 保存记录
    */
  def saveModel(conn:java.sql.Connection, tableName:String, insertSql:String, key:String, version:Int,
                dataCount: Long, model: NormalizerModel ) {
    val modelBytes = com.eurlanda.datashire.engine.util.IOUtils.readObjectToBytes(model)
    var preparedStatement : PreparedStatement = null
    try {
      preparedStatement = conn.prepareStatement(insertSql)
      preparedStatement.setLong(1, dataCount)
      preparedStatement.setBytes(2, modelBytes)
      preparedStatement.setTimestamp(3, new Timestamp(new java.util.Date().getTime))
      preparedStatement.setInt(4, version)
      preparedStatement.setObject(5, key)
      preparedStatement.execute()
      log.info("保存标准化模型到MySQL，表名：" + tableName)
    } catch {
      case e: com.mysql.jdbc.PacketTooBigException => {
        val msg = "训练模型" + modelBytes.length + "字节，太大不能写入数据表"+ tableName
        log.error(msg, e)
        log.error(e.getStackTrace())
        throw new RuntimeException(msg, e)
      }
      case e: Throwable => {
        log.error("训练模型 SQL异常", e)
        log.error(e.getStackTrace())
        throw new RuntimeException("训练模型 SQL异常", e)
      }
    }finally {
      if(preparedStatement != null){
        preparedStatement.close()
      }
    }
  }

}
