package com.eurlanda.datashire.engine.entity

import java.io.IOException
import java.sql.{PreparedStatement, SQLException, Timestamp}
import java.util

import com.eurlanda.datashire.engine.spark.mllib.regression.LinearRegressionWithElasticNetSquid
import com.eurlanda.datashire.engine.util.{CsnDataFrameUtil, DSUtil}
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.{Row, SparkSession, _}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017-08-10.
  *
  * 线性回归
  */
class TLinearRegressionWithElasticNetSquid extends TTrainSquid{

  private val log: Log = LogFactory.getLog(classOf[TLinearRegressionWithElasticNetSquid])
  setType(TSquidType.LINEAR_REGRESSION_SQUID)

  var regularization = -1.0
  var maxIter = 0
  var fitIntercept = false
  var solverIndex = -1
  var standardization = false
  var tolerance = -1.0
  var elasticNetParam = -1.0
  private var outCoefficientDataCatch: DataFrame = null //系数

  override def run(jsc: JavaSparkContext): Object = {

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

    val regression = new LinearRegressionWithElasticNetSquid()
    regression.regularization = this.regularization
    regression.maxIter = this.maxIter
    regression.fitIntercept = this.fitIntercept
    regression.solverIndex = this.solverIndex
    regression.standardization = this.standardization
    regression.tolerance = this.tolerance
    regression.training_percentage = this.getPercentage
    regression.elasticNetParam = this.elasticNetParam
    val outCoeffDataFrameArrs = new ArrayBuffer[DataFrame]()
    val hasCoeffSquid = hasCoefficientSquid
    val preRDD = preSquid.getOutRDD.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      if (key > 0) {
        // 相同的 key作为一组用于训练, 根据key分组建模，几个key就建几个模型
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
          val dataFrame = CsnDataFrameUtil.csnToRegressionLabelFeature(getJobContext.getSparkSession, groupRDD)
          val lassoRegressionOut = regression.run(dataFrame)
          saveModel(conn, tableName, saveModelSql, groupKey, modelVersion, lassoRegressionOut._1, lassoRegressionOut._2)
          //是否设置了系数squid
          if (hasCoeffSquid) {
            val outCoefficientDataCatchtmp = getCoefficientDataCatch(getJobContext.getSparkSession,
              lassoRegressionOut._1, groupKey, modelVersion)
            outCoeffDataFrameArrs.append(outCoefficientDataCatchtmp)
          }
        }
      } else {
        var dataFrame: DataFrame = null
        if (preSquid.outDataFrame != null) {
          dataFrame = preSquid.outDataFrame
        } else {
          dataFrame = CsnDataFrameUtil.csnToRegressionLabelFeature(getJobContext.getSparkSession,preRDD)
        }
        val lassoRegressionOut = regression.run(dataFrame)
        saveModel(conn, tableName, saveModelSql, key.toString, modelVersion, lassoRegressionOut._1, lassoRegressionOut._2)
        //是否设置了系数squid
        if (hasCoeffSquid) {
          val outCoefficientDataCatchtmp = getCoefficientDataCatch(getJobContext.getSparkSession, lassoRegressionOut._1,
            key.toString, modelVersion)
          outCoeffDataFrameArrs.append(outCoefficientDataCatchtmp)
        }
      }
      //是否设置了系数squid
      if (hasCoeffSquid) {
        outCoefficientDataCatch = outCoeffDataFrameArrs.reduce(_.union(_))
        return outCoefficientDataCatch
      }
      return null
    }catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error(e.getStackTrace())
        log.error("TLinearRegressionWithElasticNetSquid 异常:" + errorMessage)
        throw e
      }
    }finally {
      try
          if (conn != null && (!conn.isClosed)) conn.close()
      catch {
        case e: SQLException => {
          log.error(e)
        }
      }
      if (preRDD != null) {
        preRDD.unpersist()
      }
    }
  }

  private def saveModel(conn:java.sql.Connection, tableName:String, saveModelSql:String, key:String, version:Int,
                model:LinearRegressionModel, modelMetrics :java.util.HashMap[String,Any]) {
    val modelBytes = com.eurlanda.datashire.engine.util.IOUtils.readObjectToBytes(model)
    var preparedStatement : PreparedStatement= null
    try {
      preparedStatement = conn.prepareStatement(saveModelSql)
      //   preparedStatement.setLong(1, 0) //  //  id 是主键，已经设置自增
     preparedStatement.setLong(1, modelMetrics.get("dataCount").toString.toLong)
     preparedStatement.setFloat(2, getPercentage)
     preparedStatement.setBytes(3, modelBytes)
     preparedStatement.setFloat(4, 1.0F)
     preparedStatement.setInt(5, model.summary.totalIterations)
     preparedStatement.setInt(6, model.numFeatures)
     preparedStatement.setDouble(7, modelMetrics.get("mse").toString.toDouble)
     preparedStatement.setDouble(8, modelMetrics.get("rmse").toString.toDouble)
     preparedStatement.setDouble(9, modelMetrics.get("mae").toString.toDouble)
     preparedStatement.setDouble(10, modelMetrics.get("r2").toString.toDouble)
     preparedStatement.setDouble(11,modelMetrics.get("explainedVariance").toString.toDouble)
     preparedStatement.setString(12,modelMetrics.get("devianceResiduals").toString)
     preparedStatement.setTimestamp(13, new Timestamp(new java.util.Date().getTime))
     preparedStatement.setInt(14, version)
     preparedStatement.setObject(15, key)
     preparedStatement.execute
      log.debug("保存模型到MySQL成功， 表名：" + tableName)
    } catch {
      case e: com.mysql.jdbc.PacketTooBigException => {
        val msg = "训练模型" + modelBytes.length + "字节，太大不能写入数据表"+tableName
        log.error(msg, e)
        log.error(e.getStackTrace())
        throw new RuntimeException(msg, e)
      }
      case e: IOException => {
        log.error("训练模型序列化异常", e)
        log.error(e.getStackTrace())
        throw new RuntimeException(e)
      }
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("保持训练模型异常 "+errorMessage, e)
        log.error(e.getStackTrace())
        if(errorMessage.contains("is full")){
          throw new RuntimeException("数据库空间不足", e)
        } else if(errorMessage.contains("The size of BLOB/TEXT data inserted in one transaction is greater than 10% of redo log size")){
          throw new RuntimeException("数据大小超过日志文件的10%，请在mysql的配置文件my.cnf中增加变量innodb_log_file_size的值", e)
        }
        throw new RuntimeException("保存训练模型异常", e)
      }
    }finally {
      if(preparedStatement != null){
        preparedStatement.close()
      }
    }
  }


   override def getSaveModelSql(tableName:String): String = {
     var sb: String = null
     if (tDataSource.getType eq DataBaseType.MYSQL) {
       sb = "insert into " + tableName + "( " +
         //  "ID ," +
         "total_dataset ," +
         "training_percentage ," +
         "model," +
         "`PRECISION`," +
         "num_iterations ," +
         "Num_Features ," +
         "mse," +
         "rmse," +
         "mae," +
         "r2," +
         "explained_variance," +
         "deviance_residuals," +
         "creation_date," +
         "VERSION," +
         "`KEY`" + ") " +
         "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
     } else {
       throw new RuntimeException("不匹配的数据库")
     }
     sb
   }

  /**
    * 系数矩阵，按列排，先第一列排完 再排第二列
    * 系数从1 开始，截距从0开始
    * @param sparkSession
    * @return
    */
  private def getCoefficientDataCatch(sparkSession: SparkSession, model :LinearRegressionModel,
                              groupKey:String,modelVersion:Int): DataFrame = {
    val coeffSize = model.coefficients.size
    val seq = Array.fill(coeffSize + 1)(Tuple3(0, 0, 0.0))
    seq.update(0, Tuple3(0, 1, model.intercept)) // 截距

    for (i <- 1 to (coeffSize)) {
      seq.update(i, Tuple3(i, 1, model.coefficients.apply(i - 1))) // 系数
    }
    val coeffMatrix = sparkSession.sparkContext.makeRDD(seq)
    import sparkSession.implicits._
    val groupKeyLit = lit(groupKey)
    val modelVersionLit = lit(modelVersion)
    coeffMatrix.toDF("i", "j", "value").withColumn("key", groupKeyLit).withColumn("version", modelVersionLit)
  }


}
