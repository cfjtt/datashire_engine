package com.eurlanda.datashire.engine.entity

import java.io.IOException
import java.sql.{PreparedStatement, SQLException, Timestamp}
import java.util

import com.eurlanda.datashire.engine.spark.mllib.classification.RandomForestClassificationSquid
import com.eurlanda.datashire.engine.util.{CsnDataFrameUtil, DSUtil}
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2017-05-24.
  */
class TRandomForestClassificationSquid extends TTrainSquid {

  private val log: Log = LogFactory.getLog(classOf[TRandomForestClassificationSquid])
  setType(TSquidType.RANDOMFOREST_CLASSIFICATION_SQUID)

  var numberTrees = -1 // 树数量
  var impurityIndex = -1 //"entropy" , "gini"
  var featureSubsetStrategyIndex = -1 // auto,all,onethird,sqrt,log2,n
  var featureSubsetNValue = -1.0  // featureSubsetStrategy选 N 时的值
  var maxBins = -1  // 最大特征数
  var maxDepth = -1
  var minInfoGain = 0.0
  var subsamplingRate = 0.0
  var maxCategories = 100  // 特征最大个数
  var thresholdsCsn = ""

  override def run(jsc: JavaSparkContext): Object = {

    log.info("翻译随机森林分类")

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
      val randomForestClassification = new RandomForestClassificationSquid()
      randomForestClassification.featureSubsetStrategyIndex = this.featureSubsetStrategyIndex
      randomForestClassification.impurityIndex = this.impurityIndex
      randomForestClassification.maxBins = this.maxBins
      randomForestClassification.maxDepth = this.maxDepth
      randomForestClassification.minInfoGain = this.minInfoGain
      randomForestClassification.numberTrees = this.numberTrees
      randomForestClassification.subsamplingRate = this.subsamplingRate
      randomForestClassification.trainingDataPercentage = this.getPercentage
      randomForestClassification.featureSubsetNValue = this.featureSubsetNValue
      randomForestClassification.maxCategories = this.maxCategories
      randomForestClassification.thresholdsCsn = this.thresholdsCsn

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
          val dataFrame = CsnDataFrameUtil.csnToClassificationLabelFeature(getJobContext.getSparkSession, groupRDD)
          val randomForestClassificationOut = randomForestClassification.run(dataFrame)
          saveModel(conn,tableName,saveModelSql,kyDataCell.getData.toString,modelVersion,
            randomForestClassificationOut._1, randomForestClassificationOut._2)
        }
      } else {
        var dataFrame: DataFrame = null
        if (preSquid.outDataFrame != null) {
          dataFrame = preSquid.outDataFrame
        } else {
          dataFrame = CsnDataFrameUtil.csnToClassificationLabelFeature(getJobContext.getSparkSession, preRDD)
        }
        val randomForestClassificationOut = randomForestClassification.run(dataFrame)
        saveModel(conn,tableName,saveModelSql,key.toString,modelVersion,
          randomForestClassificationOut._1, randomForestClassificationOut._2)
      }
      return null

    } catch {
      case e: Exception => {
        val errorMessage = e.getMessage
        log.error(e.getStackTrace())
        log.error("TRandomForestClassificationSquid异常:" + errorMessage)
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
   //     "id ," +  // id 是主键，已经设置自增
        "total_dataset ," +
        "training_percentage ," +
        "uid ," +
        " MODEL," +
        "`PRECISION`," +
        "trees ," +
        "feature_importances ," +
        "num_classes ," +
        "num_features ," +
        "tree_weights ," +
        "f1 ," +
        "weighted_precision ," +
        "weighted_recall," +
        "creation_date," +
        "VERSION," +
         "`key`" + ") " +
        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    } else {
      throw new RuntimeException("不匹配的数据库")
    }
    sb
  }

  def saveModel(conn:java.sql.Connection, tableName:String, saveModelSql:String, key:String, modelVersion:Int,
                model: RandomForestClassificationModel, modelMetrics :java.util.HashMap[String,Any]) {

    val modelBytes = com.eurlanda.datashire.engine.util.IOUtils.readObjectToBytes(model)
    var preparedStatement: PreparedStatement = null
    try {
      preparedStatement = conn.prepareStatement(saveModelSql)
  //    preparedStatement.setLong(1, 0)  // id 是主键，已经设置自增
      preparedStatement.setLong(1, modelMetrics.get("dataCount").toString.toLong)
      preparedStatement.setFloat(2, getPercentage)
      preparedStatement.setString(3, model.uid)
      preparedStatement.setBytes(4, modelBytes)
      preparedStatement.setDouble(5, modelMetrics.get("precision").toString.toDouble)
      preparedStatement.setString(6, model.trees.mkString(","))
      preparedStatement.setString(7, model.featureImportances.toArray.mkString(","))
      preparedStatement.setInt(8, model.numClasses)
      preparedStatement.setInt(9,model.numFeatures)
      preparedStatement.setString(10,model.treeWeights.mkString(","))
      preparedStatement.setDouble(11, modelMetrics.get("f1").toString.toDouble)
      preparedStatement.setDouble(12, modelMetrics.get("weightedPrecision").toString.toDouble)
      preparedStatement.setDouble(13,  modelMetrics.get("weightedRecall").toString.toDouble)
      preparedStatement.setTimestamp(14, new Timestamp(new java.util.Date().getTime))
      preparedStatement.setInt(15, modelVersion)
      preparedStatement.setString(16, key)
      preparedStatement.execute
      log.debug("保存模型到MySQL成功,表名：" + tableName)
    } catch {
      case e: com.mysql.jdbc.PacketTooBigException => {
        val msg = "训练模型" + modelBytes.length + "字节，太大不能写入数据表"+ tableName
        log.error(msg, e)
        log.error(e.getStackTrace())
        throw new RuntimeException(msg, e)
      } case e: IOException => {
        log.error("训练模型序列化异常", e)
        log.error(e.getStackTrace())
        throw new RuntimeException(e)
      } case e: Throwable => {
        val errorMessage = e.getMessage
        log.error("保存训练模型异常"+errorMessage, e)
        log.error(e.getStackTrace())
        if(errorMessage.contains("is full")){
          throw new RuntimeException("数据库空间不足", e)
        }else if(errorMessage.contains("The size of BLOB/TEXT data inserted in one transaction is greater than 10% of redo log size")){
          throw new RuntimeException("数据大小超过日志文件的10%，请在mysql的配置文件my.cnf中增加变量innodb_log_file_size的值", e)
        }
        throw new RuntimeException("保存模型异常", e)
      }
    }finally {
      if(preparedStatement != null){
        preparedStatement.close()
      }
    }
  }

}


object TRandomForestClassificationSquid{

  /**
    * 系数矩阵，按列排，先第一列排完 再排第二列
    * 系数从1 开始，截距从0开始
    * @param sparkSession
    * @return
    */
  def getFeatureImportancesDataCatch(sparkSession: SparkSession, featureImportances :Vector,
                                     groupKey:String,modelVersion:Int): DataFrame = {
    val coeffSize = featureImportances.size
    val seq = Array.fill(coeffSize)(Tuple3(0, 0, 0.0))
    for (i <- 1 to (coeffSize)) {
      seq.update(i, Tuple3(i, 1, featureImportances.apply(i - 1))) // 系数
    }
    val coeffMatrix = sparkSession.sparkContext.makeRDD(seq)
    import sparkSession.implicits._
    val groupKeyLit = lit(groupKey)
    val modelVersionLit = lit(modelVersion)
    coeffMatrix.toDF("i", "j", "value").withColumn("key", groupKeyLit).withColumn("version", modelVersionLit)
  }

}
