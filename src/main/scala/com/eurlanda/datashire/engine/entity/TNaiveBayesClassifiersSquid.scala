package com.eurlanda.datashire.engine.entity

import java.io.IOException
import java.sql.PreparedStatement
import java.util
import java.util.Map

import com.eurlanda.datashire.engine.spark.mllib.classification.NaiveBayesClassifiersSquid
import com.eurlanda.datashire.engine.util.{CsnDataFrameUtil, DSUtil, ExceptionUtil}
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  *
  */
class TNaiveBayesClassifiersSquid extends TTrainSquid {

  private val log: Log = LogFactory.getLog(classOf[TNaiveBayesClassifiersSquid])
  setType(TSquidType.NAIVEBAYS_TRAIN_SQUID)

  var modelTypeIndex = -1 //  multinomial, bernoulli
  var smoothingParameter = 0.0
  var thresholdsCsn = ""

  private var outCoefficientDataCatch: DataFrame = null //系数

  override def run(jsc: JavaSparkContext): Object = {

    log.info("翻译朴素贝叶斯分类Squid")

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
    val saveModelSql = getSaveModelSql(tableName)
    val modelVersion = init(conn,tableName)

    val naiveBayesClassifiersSquid = new NaiveBayesClassifiersSquid()
    naiveBayesClassifiersSquid.smoothingParameter = this.smoothingParameter
    naiveBayesClassifiersSquid.modelTypeIndex = this.modelTypeIndex
    naiveBayesClassifiersSquid.trainDataPercentage = this.getPercentage
    naiveBayesClassifiersSquid.thresholdsCsn = this.thresholdsCsn

    val outCoeffDataFrameArrs = new ArrayBuffer[DataFrame]()
    val hasCoeffSquid = hasCoefficientSquid
    val preRDD = preSquid.getOutRDD.persist(StorageLevel.MEMORY_AND_DISK)
    try {
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
          val groupKey = kyDataCell.getData.toString
          val dataFrame = CsnDataFrameUtil.csnToClassificationLabelFeature(getJobContext.getSparkSession, groupRDD)
          val mpcOut = naiveBayesClassifiersSquid.run(dataFrame)
          saveModel(conn, tableName, saveModelSql, groupKey, modelVersion, mpcOut._1, mpcOut._2)
          //是否设置了系数squid
          if (hasCoeffSquid) {
            val outCoefficientDataCatchtmp = getCoefficientDataCatch(getJobContext.getSparkSession,
              mpcOut._1, groupKey, modelVersion)
            outCoeffDataFrameArrs.append(outCoefficientDataCatchtmp)
          }
        }
      } else {
        var dataFrame: DataFrame = null
        if (preSquid.outDataFrame != null) {
          dataFrame = preSquid.outDataFrame //.persist(StorageLevel.MEMORY_AND_DISK)
        } else {
          dataFrame = CsnDataFrameUtil.csnToClassificationLabelFeature(getJobContext.getSparkSession, preRDD)
        }
        val mpcOut = naiveBayesClassifiersSquid.run(dataFrame)
        saveModel(conn, tableName, saveModelSql, key.toString, modelVersion, mpcOut._1, mpcOut._2)
        //是否设置了系数squid
        if (hasCoeffSquid) {
          val outCoefficientDataCatchtmp = getCoefficientDataCatch(getJobContext.getSparkSession, mpcOut._1, key + "", modelVersion)
          outCoeffDataFrameArrs.append(outCoefficientDataCatchtmp)
        }
      }
      //是否设置了系数squid
      if (hasCoeffSquid) {
        outCoefficientDataCatch = outCoeffDataFrameArrs.reduce(_.union(_))
        return outCoefficientDataCatch
      }
      return null
    } catch {
      case e: Throwable => {
        log.error("TNaiveBayesClassifiersSquid 异常:" + e.getMessage)
        log.error(e.getStackTrace())
        val exp = ExceptionUtil.translateExceptionMessage(e)
        if(exp != null){
          throw exp
        }
        throw e
      }
    } finally {
      if (conn != null) {
        conn.close()
      }
      if (preRDD != null) {
        preRDD.unpersist()
      }

    }
  }

  /**
    * 系数矩阵，按列排，先第一列排完 再排第二列
    * 系数从1 开始，截距从0开始
    * @param sparkSession
    * @return
    */
  private def getCoefficientDataCatch(sparkSession: SparkSession, model :NaiveBayesModel,
                                      groupKey:String,modelVersion:Int): DataFrame = {
    val seq = new ArrayBuffer[(Int, Int, Double)]()
    for (i <- 0 until (model.theta.numRows); j <- 0 until (model.theta.numCols)) { // 系数矩阵，大小 numLabels * numFeatures
      seq.append((i + 1, j + 1, model.theta.apply(i, j)))
    }
    val coeffMatrix = sparkSession.sparkContext.makeRDD(seq)
    import sparkSession.implicits._
    val groupKeyLit = lit(groupKey)
    val modelVersionLit = lit(modelVersion)
    coeffMatrix.toDF("i", "j", "value").withColumn("key", groupKeyLit).withColumn("version", modelVersionLit)
  }

  /**
    * 保存记录
    */
  private def saveModel(conn:java.sql.Connection, tableName:String, saveModelSql:String, key:String, version:Int,
                        model: NaiveBayesModel, modelMetrics :java.util.HashMap[String,Any]) {
    val modelBytes = com.eurlanda.datashire.engine.util.IOUtils.readObjectToBytes(model)
    var preparedStatement : PreparedStatement= null
    try {
      preparedStatement = conn.prepareStatement(saveModelSql)
      // preparedStatement.setLong(1, 0)  // id 是主键，已经设置自增
      preparedStatement.setLong(1, modelMetrics.get("dataCount").toString.toLong)
      preparedStatement.setFloat(2, this.getPercentage)
      preparedStatement.setBytes(3, modelBytes)
      preparedStatement.setFloat(4, modelMetrics.get("precision").toString.toFloat)
      preparedStatement.setInt(5, model.numClasses)
      preparedStatement.setInt(6, model.numFeatures)
      preparedStatement.setString(7, model.pi.toArray.mkString(","))
      preparedStatement.setDouble(8, modelMetrics.get("f1").toString.toDouble)
      preparedStatement.setDouble(9, modelMetrics.get("weightedPrecision").toString.toDouble)
      preparedStatement.setDouble(10,modelMetrics.get("weightedRecall").toString.toDouble)
      preparedStatement.setTimestamp(11, new java.sql.Timestamp(new java.util.Date().getTime))
      preparedStatement.setInt(12, version)
      preparedStatement.setString(13, key)
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
        throw new RuntimeException(e)
      }
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("保存训练模型异常" + errorMessage, e)
        log.error(e.getStackTrace())
        if (errorMessage.contains("is full")) {
          throw new RuntimeException("数据库空间不足，请删除无用表", e)
        } else if (errorMessage.contains("Increase the redo log size using innodb_log_file_size")) {
          throw new RuntimeException("数据BLOB/TEXT超过redo日志文件的10% ，请增加redo日志文件大小", e)
        }else if(errorMessage.contains("Unknown column") || errorMessage.contains("in 'field list'")){
          val msg = errorMessage.replaceAll("Unknown column ","落地表不存在字段").replaceAll(" in 'field list'","")
          throw new RuntimeException(msg, e)
        }else if(errorMessage.contains("Could not create connection to database server")){
          throw new RuntimeException("保存模型时不能连接到数据库", e)
        }
        throw new RuntimeException("保存模型异常", e)
      }
    }finally {
      if(preparedStatement != null){
        preparedStatement.close()
      }
    }
  }

  protected override def getSaveModelSql(tableName: String): String = {
    var sb: String = null
    if (tDataSource.getType eq DataBaseType.MYSQL) {
      sb = "insert into " + tableName + "( " +
        //  "id," +    //id 是主键，已经设置自增
        "total_dataset," +
        "training_percentage," +
        "model," +
        "`precision`," +
        "num_Classes," +
        "num_features ," +
        "pi ," +
        "f1 ," +
        "weighted_Precision ," +
        "weighted_Recall ," +
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
