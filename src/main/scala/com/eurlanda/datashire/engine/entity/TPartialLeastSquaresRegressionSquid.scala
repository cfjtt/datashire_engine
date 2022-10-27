package com.eurlanda.datashire.engine.entity

import java.io.IOException
import java.sql.PreparedStatement
import java.util

import com.eurlanda.datashire.engine.spark.mllib.normalize.NormalizerModel
import com.eurlanda.datashire.engine.util.{ConstantUtil, DSUtil, SquidUtil}
import com.eurlanda.datashire.enumeration.{DataBaseType, SquidTypeEnum}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.ml.regression.{PartialLeastSquaresRegression, PartialLeastSquaresRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017-06-30
  */
class TPartialLeastSquaresRegressionSquid extends TTrainSquid {

  private val log: Log = LogFactory.getLog(classOf[TPartialLeastSquaresRegressionSquid])
  setType(TSquidType.PARTIALLEASTSQUARES_SQUID)

  var maxIter = -1
  var tolerance =0.0000001
  var componentCount = -1
  var XNormalizerModel : NormalizerModel = null
  var YNormalizerModel : NormalizerModel = null
  var xNormalizerModelSql :String = null
  var yNormalizerModelSql :String = null

  private var XNormalizerModelColumnMean :Array[Double] = null
  private var XNormalizerModelColumnStd :Array[Double] = null
  private var YNormalizerModelColumnMean :Array[Double] = null
  private var YNormalizerModelColumnStd :Array[Double] = null
  private var outCoefficientDataCatch: DataFrame = null //系数

  override def run(jsc: JavaSparkContext): Object = {
    if (outCoefficientDataCatch != null) {
      outDataFrame = outCoefficientDataCatch
      return outDataFrame
    }
    log.info("翻译PartialLeastSquaresRegressionSquid : Squidid = " + getSquidId)

    if (preSquid.getOutRDD == null) {
      preSquid.runSquid(jsc)
    }
    val tableName = this.getTableName
    val saveModelSql = getSaveModelSql(tableName)
    val modelVersion = init()

    setNormalizerMeanAndStd()

    //   val dataCatchStructType = getDataCatchStructType()
    val pls = new PartialLeastSquaresRegression()
      .setMaxIter(this.maxIter)
      .setTol(this.tolerance)
      .setComponentsCount(this.componentCount)
      .setXNormalizerModelColumnMean(XNormalizerModelColumnMean)
      .setXNormalizerModelColumnStd(XNormalizerModelColumnStd)
      .setYNormalizerModelColumnMean(YNormalizerModelColumnMean)
      .setYNormalizerModelColumnStd(YNormalizerModelColumnStd)
      .setTrainPercentage(getPercentage)
    val outCoeffArr = new ArrayBuffer[DataFrame]()
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
          val dataFrame = rdd2DataFrame(getJobContext.getSparkSession, groupRDD)
          val plsModel = pls.train2(dataFrame)
          val groupKey = kyDataCell.getData.toString

          var conn: java.sql.Connection = null
          try {
            conn = getConnectionFromDS
            saveModel(conn, tableName, saveModelSql, groupKey, modelVersion, plsModel._1, plsModel._2)
          } catch {
            case e: Throwable => {
              log.error("保存PLS模型错误", e)
              throw new RuntimeException("保存PLS模型错误", e)
            }
          } finally {
            if (conn != null) {
              conn.close()
            }
          }
          val outCoefficientDataCatchtmp = getCoefficientDataCatch(getJobContext.getSparkSession, plsModel._2,
            groupKey, modelVersion)
          outCoeffArr.append(outCoefficientDataCatchtmp)
        }
      } else {
        var dataFrame: DataFrame = null
        if (preSquid.outDataFrame != null) {
          dataFrame = preSquid.outDataFrame
        } else {
          dataFrame = rdd2DataFrame(getJobContext.getSparkSession,preRDD)
        }
        val plsModel = pls.train2(dataFrame)
        val groupKey = key + ""

        var conn: java.sql.Connection = null
        try {
          conn = getConnectionFromDS
          saveModel(conn, tableName, saveModelSql, groupKey, modelVersion, plsModel._1, plsModel._2)
        } catch {
          case e: Throwable => {
            log.error("保存PLS模型错误", e)
            throw new RuntimeException("保存PLS模型错误", e)
          }
        } finally {
          if (conn != null) {
            conn.close()
          }
        }
        val outCoefficientDataCatchtmp = getCoefficientDataCatch(getJobContext.getSparkSession, plsModel._2,
          groupKey, modelVersion)
        outCoeffArr.append(outCoefficientDataCatchtmp)
      }
      outCoefficientDataCatch = outCoeffArr.reduce(_.union(_))
      return outCoefficientDataCatch
    } catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("TPartialLeastSquaresRegression 异常:" + errorMessage)
        log.error(e.getStackTrace())
        if (errorMessage.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源，请稍后重试或在配置文件中调大配置项：" +
            "SPARK_EXECUTOR_MEMORY，SPARK_DRIVER_MEMORY", e)
        } else if (errorMessage.contains("The size of BLOB/TEXT data inserted in one transaction is greater than 10% of redo log size")) {
          throw new RuntimeException("数据大小超过日志文件的10%，请在mysql的配置文件my.cnf中增加变量innodb_log_file_size的值", e)
        }
        throw e
      }
    } finally {
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
  def getCoefficientDataCatch(sparkSession: SparkSession, plsModel :PartialLeastSquaresRegressionModel,
                              groupKey:String,modelVersion:Int): DataFrame = {
    val coeffMatrix = plsModel.coefficient.entries.mapPartitions(par =>
      par.map(ent => (ent.i+1, ent.j +1 , ent.value))) // 系数从1 开始，截距从0开始
    import sparkSession.implicits._
    val groupKeyLit = lit(groupKey)
    val modelVersionLit = lit(modelVersion)
    coeffMatrix.toDF("i", "j", "value").withColumn("key", groupKeyLit).withColumn("version", modelVersionLit)
  }

  /**
    * 系数
    * (i, j, value)
    * @return
    */
  def getCoefficientDataCatchStructType(): StructType = {
    val fields = new Array[StructField](3)
    fields.update(0, StructField("i", LongType))
    fields.update(1, StructField("j", LongType))
    fields.update(2, StructField("value", DoubleType))
    StructType(fields)
  }

  /**
    *
    * @param sparkSession
    * @param dataViewStructType
    * @return
    */
  def getDataCatchDataFrame(sparkSession: SparkSession, plsModel :PartialLeastSquaresRegressionModel,
                            dataViewStructType:StructType): DataFrame = {
    var xWeightCsnRdd = plsModel.xWeight.toRowMatrix().rows.mapPartitions(par=>par.map(row => row.toArray.mkString(",")))
    var yWeightCsnRdd = plsModel.yWeight.toRowMatrix().rows.mapPartitions(par=>par.map(row => row.toArray.mkString(",")))
    var xScoreCsnRdd = plsModel.xScore.toRowMatrix().rows.mapPartitions(par=>par.map(row => row.toArray.mkString(",")))
    var yScoreCsnRdd = plsModel.yScore.toRowMatrix().rows.mapPartitions(par=>par.map(row => row.toArray.mkString(",")))
    var xLoadingCsnRdd = plsModel.xLoading.toRowMatrix().rows.mapPartitions(par=>par.map(row => row.toArray.mkString(",")))
    var yLoadingCsnRdd = plsModel.yLoading.toRowMatrix().rows.mapPartitions(par=>par.map(row => row.toArray.mkString(",")))
    var xRotationCsnRdd = plsModel.xRotation.toRowMatrix().rows.mapPartitions(par=>par.map(row => row.toArray.mkString(",")))
    var yRotationCsnRdd = plsModel.yRotation.toRowMatrix().rows.mapPartitions(par=>par.map(row => row.toArray.mkString(",")))

    val xWeightCsnRddCount = plsModel.XTrainColumnCount
    val yWeightCsnRddCount = plsModel.YTrainColumnCount
    val xScoreCsnRddCount = plsModel.trainDataCount
    val yScoreCsnRddCount = plsModel.trainDataCount
    val xLoadingCsnRddCount = plsModel.XTrainColumnCount
    val yLoadingCsnRddCount = plsModel.YTrainColumnCount
    val xRotationCsnRddCount = plsModel.XTrainColumnCount
    val yRotationCsnRddCount = plsModel.YTrainColumnCount

    val maxRowCount = Array(xWeightCsnRddCount, yWeightCsnRddCount,
      xScoreCsnRddCount, yScoreCsnRddCount,
      xLoadingCsnRddCount, yLoadingCsnRddCount,
      xRotationCsnRddCount, yRotationCsnRddCount
    ).max

    //不足最大行的要填充
    val sparkContext = sparkSession.sparkContext
    if (xScoreCsnRddCount < maxRowCount) {
      val emptyrdd = createEmptyRdd(sparkContext, maxRowCount - xScoreCsnRddCount)
      xScoreCsnRdd = xScoreCsnRdd.union(emptyrdd)
    }
    if (yScoreCsnRddCount < maxRowCount) {
      val emptyrdd = createEmptyRdd(sparkContext, maxRowCount - yScoreCsnRddCount)
      yScoreCsnRdd = yScoreCsnRdd.union(emptyrdd)
    }
    if (xWeightCsnRddCount < maxRowCount) {
      val emptyrdd = createEmptyRdd(sparkContext, maxRowCount - xWeightCsnRddCount)
      xWeightCsnRdd = xWeightCsnRdd.union(emptyrdd)
    }
    if (yWeightCsnRddCount < maxRowCount) {
      val emptyrdd = createEmptyRdd(sparkContext, maxRowCount - yWeightCsnRddCount)
      yWeightCsnRdd = yWeightCsnRdd.union(emptyrdd)
    }
    if (xLoadingCsnRddCount < maxRowCount) {
      val emptyrdd = createEmptyRdd(sparkContext, maxRowCount - xLoadingCsnRddCount)
      xLoadingCsnRdd = xLoadingCsnRdd.union(emptyrdd)
    }
    if (yLoadingCsnRddCount < maxRowCount) {
      val emptyrdd = createEmptyRdd(sparkContext, maxRowCount - yLoadingCsnRddCount)
      yLoadingCsnRdd = yLoadingCsnRdd.union(emptyrdd)
    }
    if (xRotationCsnRddCount < maxRowCount) {
      val emptyrdd = createEmptyRdd(sparkContext, maxRowCount - xRotationCsnRddCount)
      xRotationCsnRdd = xRotationCsnRdd.union(emptyrdd)
    }
    if (yRotationCsnRddCount < maxRowCount) {
      val emptyrdd = createEmptyRdd(sparkContext, maxRowCount - yRotationCsnRddCount)
      yRotationCsnRdd = yRotationCsnRdd.union(emptyrdd)
    }
    import sparkSession.implicits._
    val xWeightCsnRddId = xWeightCsnRdd.zipWithIndex().toDF(dataViewStructType.apply(0).name, "id")
    val yWeightCsnRddId = yWeightCsnRdd.zipWithIndex().toDF(dataViewStructType.apply(1).name, "id")
    val xScoreCsnRddId = xScoreCsnRdd.zipWithIndex().toDF(dataViewStructType.apply(2).name, "id")
    val yScoreCsnRddId = yScoreCsnRdd.zipWithIndex().toDF(dataViewStructType.apply(3).name, "id")
    val xLoadingCsnRddId = xLoadingCsnRdd.zipWithIndex().toDF(dataViewStructType.apply(4).name, "id")
    val yLoadingCsnRddId = yLoadingCsnRdd.zipWithIndex().toDF(dataViewStructType.apply(5).name, "id")
    val xRotationCsnRddId = xRotationCsnRdd.zipWithIndex().toDF(dataViewStructType.apply(6).name, "id")
    val yRotationCsnRddId = yRotationCsnRdd.zipWithIndex().toDF(dataViewStructType.apply(7).name, "id")

    val joindf = xWeightCsnRddId.join(yWeightCsnRddId, "id")
      .join(xScoreCsnRddId, "id")
      .join(yScoreCsnRddId, "id")
      .join(xLoadingCsnRddId, "id")
      .join(yLoadingCsnRddId, "id")
      .join(xRotationCsnRddId, "id")
      .join(yRotationCsnRddId, "id")
      .sort("id")

    val columnname = dataViewStructType.map(col => col.name).toArray
    joindf.select(columnname.head, columnname.tail: _ *)
  }

  /**
    * 创建空字符串rdd
    * @param sparkContext
    * @param count
    * @return
    */
  def createEmptyRdd(sparkContext:SparkContext,count:Long): RDD[String] = {
    sparkContext.range(0, count).mapPartitions(par => par.map(row => ""))
  }

  /**
    *
    * @return
    */
  def getDataCatchStructType(): StructType = {
    val fields = new Array[StructField](8)
    fields.update(0,StructField("XWeight",StringType))
    fields.update(1,StructField("YWeight",StringType))
    fields.update(2,StructField("XScore",StringType))
    fields.update(3,StructField("YScore",StringType))
    fields.update(4,StructField("XLoading",StringType))
    fields.update(5,StructField("YLoading",StringType))
    fields.update(6,StructField("XRotation",StringType))
    fields.update(7,StructField("YRotation",StringType))
    StructType(fields)
  }

  def setNormalizerMeanAndStd(): Unit = {
    if (xNormalizerModelSql == null ) {
      throw new RuntimeException("查询X标准化模型语句是null")
    }
    if (yNormalizerModelSql == null) {
      throw new RuntimeException("查询Y标准化模型语句是null")
    }

    XNormalizerModel = getPLSNormailzerModel(xNormalizerModelSql)
    YNormalizerModel = getPLSNormailzerModel(yNormalizerModelSql)

    if (XNormalizerModel == null ) {
      throw new RuntimeException("X标准化模型是null")
    }
    if (YNormalizerModel == null) {
      throw new RuntimeException("Y标准化模型是null")
    }
    if (! XNormalizerModel.method.equalsIgnoreCase("Standard") ) {
      throw new RuntimeException("X标准化方法不是Standard")
    }
    if (!YNormalizerModel.method.equalsIgnoreCase("Standard")) {
      throw new RuntimeException("Y标准化方法不是Standard")
    }
    val xModel = XNormalizerModel.normalizerModeObject.asInstanceOf[StandardScalerModel]
    XNormalizerModelColumnMean = xModel.mean.toArray
    XNormalizerModelColumnStd = xModel.std.toArray

    val yModel = YNormalizerModel.normalizerModeObject.asInstanceOf[StandardScalerModel]
    YNormalizerModelColumnMean = yModel.mean.toArray
    YNormalizerModelColumnStd = yModel.std.toArray
  }

  /**
    * 把csn拆分成 features
    * @param session
    * @param csnRdd
    * @return  返回一列 features
    */
  private def rdd2DataFrame(session: SparkSession, csnRdd: RDD[java.util.Map[Integer, DataCell]]): DataFrame = {
    val data = csnRdd.mapPartitions(par => {
      par.map { row =>
        val rowArray = collection.mutable.ArrayBuffer[Any]()
        for (i <- 0.until(row.keySet().size())) {
          val dt = row.get(row.keySet().toArray.apply(i)).getData.toString
          rowArray.append(dt)
        }
        Row.fromSeq(rowArray)
      }
    })

    val fields = new Array[StructField](1)
    fields.update(0, StructField("yx", org.apache.spark.sql.types.StringType))
    val schema = StructType(fields)
    val df = session.createDataFrame(data, schema)
    return df
  }

  protected override def getSaveModelSql(tableName: String): String = {
    var sb: String = null
    if (this.tDataSource.getType eq DataBaseType.MYSQL) {
      sb = "insert into " + tableName + "( " +
      //  "ID ," +  // id 是主键，已经设置自增
        "total_dataset ," +
        "training_percentage ," +
        "model ," +
         "`PRECISION`," +
        "MSE ," +
        "RMSE ," +
        "R2," +
        "MAE," +
        "explained_variance," +
        "creation_date," +
        "VERSION," +
        "`key`"+ ") " +
        "values(?,?,?,?,?,?,?,?,?,?,?,?)"
    } else {
      throw new RuntimeException("不匹配的数据库")
    }
    sb
  }

  /**
    * 保存记录
    */
 protected  def saveModel(conn:java.sql.Connection, tableName:String, saveModelSql:String, key:String,
                          modelVersion:Int, totalDataset: Long,
                          model: PartialLeastSquaresRegressionModel) {
    val modelBytes = com.eurlanda.datashire.engine.util.IOUtils.readObjectToBytes(model)
    var preparedStatement: PreparedStatement = null
    try {
      preparedStatement = conn.prepareStatement(saveModelSql)
      //id 是主键，已经设置自增，不需赋值
     preparedStatement.setLong(1, totalDataset)
     preparedStatement.setFloat(2, getPercentage)
     preparedStatement.setBytes(3, modelBytes)
     preparedStatement.setFloat(4, 1.0F)
     preparedStatement.setString(5, model.trainingSummary.mse)
     preparedStatement.setString(6, model.trainingSummary.rmse)
     preparedStatement.setString(7, model.trainingSummary.r2)
     preparedStatement.setString(8, model.trainingSummary.mae)
     preparedStatement.setString(9, model.trainingSummary.explainedVariance)
     preparedStatement.setTimestamp(10, new java.sql.Timestamp(new java.util.Date().getTime))
     preparedStatement.setInt(11, modelVersion)
     preparedStatement.setString(12, key)
     preparedStatement.execute
      log.info("保存PLS模型到MySQL,表名：" + tableName)
    } catch {
      case e: com.mysql.jdbc.PacketTooBigException => {
        val msg = "训练模型" + modelBytes.length + "字节太大,不能写入数据表"+tableName
        log.error(msg, e)
        log.error(e.getStackTrace())
        throw new RuntimeException(msg, e)
      }
      case e: IOException => {
        log.error("训练模型序列化异常..", e)
        throw new RuntimeException(e)
      }
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("训练模型 SQL异常"+errorMessage, e)
        log.error(e.getStackTrace())
        if(errorMessage.contains("is full")){
          throw new RuntimeException("数据库空间不足，请删除无用表", e)
        }else if(errorMessage.contains("Increase the redo log size using innodb_log_file_size")){
          throw new RuntimeException("数据BLOB/TEXT超过redo日志文件的10% ，请增加redo日志文件大小", e)
        }
        throw new RuntimeException("保存PLS模型错误", e)
      }
    }finally {
      if(preparedStatement != null){
        preparedStatement.close()
      }
    }
  }

  /**
    * 运行时读取模型
    * @param modelSql
    * @return
    */
  private def getPLSNormailzerModel(modelSql: String): NormalizerModel = {
    var modelObj: NormalizerModel = null
    try {
      val mapData = ConstantUtil.getDataMiningJdbcTemplate.queryForMap(modelSql)
      val bytes = mapData.get("MODEL").asInstanceOf[Array[Byte]]
      modelObj = SquidUtil.genModel(bytes, SquidTypeEnum.NORMALIZER).asInstanceOf[NormalizerModel]
      modelObj
    } catch {
      case e: Throwable => {
        val errmsg: String = e.getMessage
        if (errmsg.contains("Incorrect result size:")) throw new RuntimeException("读取标准化模型失败", e)
        throw e
      }
    }
  }


}
