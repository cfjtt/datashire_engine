package com.eurlanda.datashire.engine.entity

import java.sql.{Connection, PreparedStatement, Timestamp}
import java.util
import java.util.Properties

import com.eurlanda.datashire.common.util.HbaseUtil
import com.eurlanda.datashire.engine.spark.mllib.associationrules.AssociationRulesSquid
import com.eurlanda.datashire.engine.util.DSUtil
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel


/**
  * Created by hwj on 2016-05-11
  */
class TAssociationRulesTrainSquid extends TTrainSquid {

  private val log: Log = LogFactory.getLog(classOf[TAssociationRulesTrainSquid])
  setType(TSquidType.ASSOCIATIONRULES_SQUID)

  var minSupport = 0.0
  var minConfidence = 0.0

  override def run(jsc: JavaSparkContext): Object = {
    if (preSquid.getOutRDD == null) {
      preSquid.runSquid(jsc)
    }
    val modelVersion = init
    val tableName = getTableName
 //   val saveModelSql = getSaveModelSql(tableName)
    val preRDD = preSquid.getOutRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val associationRulesSquid = new AssociationRulesSquid()
    associationRulesSquid.minSupport = this.minSupport
    associationRulesSquid.minConfidence = this.minConfidence
    try {
      if (key > 0) {
        val list = preRDD.rdd.map(tmpmap => tmpmap.get(key)).distinct().collect()
        list.foreach { dc =>
          val filterRDD = preRDD.rdd.filter(tmpmap => {
            if (DSUtil.isNull(dc)) {
              DSUtil.isNull(tmpmap.get(key))
            } else {
              dc == tmpmap.get(key)
            }
          })
          var model = associationRulesSquid.run(getItemRdd(inKey, filterRDD), getJobContext.getSparkSession)
          model = addColumnDataFrame(getJobContext.getSparkSession, dc.getData.toString, modelVersion, model)
         // saveModel(tableName, saveModelSql, dc.getData.toString, modelVersion, model)
           saveModel(tableName, model)
        }
      } else {
        var model = associationRulesSquid.run(getItemRdd(inKey, preRDD), getJobContext.getSparkSession)
        model = addColumnDataFrame(getJobContext.getSparkSession, key + "", modelVersion, model)
      //  saveModel(tableName, saveModelSql, key + "", modelVersion, model)
          saveModel(tableName,model)
      }
      return null
    } catch {
      case e: Throwable => {
        val errorMessage = e.getMessage
        log.error(e.getStackTrace)
        if (errorMessage.contains("The size of BLOB/TEXT data inserted in one transaction is greater than 10% of redo log size")) {
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
    * RDD的每个元素都是一个数组，每个数组表示一条记录
    * @param inKey
    * @param preRDD
    * @return
    */
  private def getItemRdd(inKey: Int, preRDD: JavaRDD[util.Map[Integer, DataCell]]): RDD[scala.Array[String]] = {
    val dataRDD = preRDD.rdd.filter(x => x.get(inKey).getData != null)
      .map(x => x.get(inKey).getData.toString)
    val splitedRDD = dataRDD.map(x => x.split(",").map(x => x.trim).distinct)
      .filter(x => x != null && x.length > 0)
    splitedRDD
  }

  /**
    *
    * @param itemsAssociationRules
    * @param tableName
    * @param version
    * @param key
    */
  private def saveModel(tableName: String, saveModelSql: String, key: String, version: Int,
                        itemsAssociationRules: DataFrame) {
    try {
      itemsAssociationRules.foreachPartition(partition => {
        var connection: Connection = null
        var preparedStatement: PreparedStatement = null
        try {
          // connection = ConstantUtil.getDataMiningJdbcTemplate.getDataSource.getConnection
          connection = getConnectionFromDS
          partition.foreach(x => {
            preparedStatement = connection.prepareStatement(saveModelSql)
            //// preparedStatement.setInt(0, x.getOrElse("ID", "0").toInt) // ID 列不插入到数据表，主键，自增
            /*  preparedStatement.setInt(1, x.getOrElse("total_dataset", "0").toInt)
          preparedStatement.setInt(2, x.getOrElse("rule_size", "0").toInt)
          preparedStatement.setString(3, x.getOrElse("antecedent", null))
          preparedStatement.setString(4, x.getOrElse("consequent", null))
          preparedStatement.setInt(5, x.getOrElse("antecedent_size", "0").toInt)
          preparedStatement.setInt(6, x.getOrElse("antecedent_instance_size", "0").toInt)
          preparedStatement.setFloat(7, x.getOrElse("antecedent_support", "0.0").toFloat)
          preparedStatement.setInt(8, x.getOrElse("consequent_size", "0").toInt)
          preparedStatement.setInt(9, x.getOrElse("consequent_instance_size", "0").toInt)
          preparedStatement.setFloat(10, x.getOrElse("consequent_support", "0.0").toFloat)
          preparedStatement.setFloat(11, x.getOrElse("rule_support", "0.0").toFloat)
          preparedStatement.setFloat(12, x.getOrElse("confidence", "0.0").toFloat)
          preparedStatement.setFloat(13, x.getOrElse("lift", "0.0").toFloat)
          preparedStatement.setFloat(14, x.getOrElse("deployment_capability", "0.0").toFloat)
          preparedStatement.setTimestamp(15, new Timestamp(new java.util.Date().getTime))
          preparedStatement.setInt(16, version)
          preparedStatement.setString(17, key) */

            preparedStatement.setLong(1, x.getLong(0)) //total_dataset
            preparedStatement.setLong(2, x.getLong(1)) // rule_size
            preparedStatement.setString(3, x.getString(2)) //antecedent
            preparedStatement.setString(4, x.getString(3)) //consequent
            preparedStatement.setInt(5, x.getInt(4)) //antecedent_size
            preparedStatement.setInt(6, x.getInt(5)) // antecedent_instance_size
            preparedStatement.setDouble(7, x.getDouble(6)) //antecedent_support
            preparedStatement.setInt(8, x.getInt(7)) //consequent_size
            preparedStatement.setInt(9, x.getInt(8)) // consequent_instance_size
            preparedStatement.setDouble(10, x.getDouble(9)) //consequent_support
            preparedStatement.setDouble(11, x.getDouble(10)) //rule_support
            preparedStatement.setDouble(12, x.getDouble(11)) //confidence
            preparedStatement.setDouble(13, x.getDouble(12)) //lift
            preparedStatement.setDouble(14, x.getDouble(13)) //deployment_capability
            preparedStatement.setTimestamp(15, new Timestamp(new java.util.Date().getTime))
            preparedStatement.setInt(16, version)
            preparedStatement.setString(17, key)

            preparedStatement.execute()

          })
        } catch {
          case e: Throwable =>
            log.error("保存关联规则模型异常")
            log.error(e.getMessage)
            log.error(e.getStackTrace())
            throw e
        } finally {
          if (preparedStatement != null) {
            preparedStatement.close()
          }
          if (connection != null) {
            connection.close()
          }
        }
      })
      log.debug("保存模型到 MySQl 成功, 表名：" + tableName)
    } catch {
      case e: Throwable => {
        log.error("保存模型失败" + e.getMessage)
        log.error(e.getStackTrace)
        throw new RuntimeException("没有足够资源，请稍后再试")
      }
    }
  }

  /**
    * 添加列 creation_date，version，key
    *
    * @param sparkSession
    * @param groupKey
    * @param modelVersion
    * @param itemsAssociationRules
    * @return
    */
  private def addColumnDataFrame(sparkSession: SparkSession, groupKey: String, modelVersion: Int,
                                 itemsAssociationRules: DataFrame): DataFrame = {
    val groupKeyLit = lit(groupKey)
    val modelVersionLit = lit(modelVersion)
    val creationDateLit = lit(new java.sql.Timestamp(new java.util.Date().getTime))
    itemsAssociationRules
      .withColumn("creation_date", creationDateLit)
      .withColumn("version", modelVersionLit)
      .withColumn("key", groupKeyLit)
  }

  /**
    *
    * @param itemsAssociationRules
    * @param tableName
    */
  private def saveModel(tableName: String, itemsAssociationRules: DataFrame) {
    val url = "jdbc:mysql://" + tDataSource.getHost + "/" + tDataSource.getDbName
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", tDataSource.getUserName)
    connectionProperties.setProperty("password", tDataSource.getPassword)
    //  connectionProperties.setProperty("batchsize","1000")
     connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")
    try {
      itemsAssociationRules.write.mode(SaveMode.Append).jdbc(url, tableName, connectionProperties)
      log.debug("保存模型成功, 表名：" + tableName)
    } catch {
      case e: Throwable => {
        log.error("保存关联规则模型异常," + url + "." + tableName)
        log.error(e.getMessage)
        log.error(e.getStackTrace())
        throw e
      }
    }
  }

  protected override def getSaveModelSql(tableName: String): String = {
    var sb: String = null
    val defaultPkName: String = HbaseUtil.DEFAULT_PK_NAME
    if (tDataSource.getType eq DataBaseType.HBASE_PHOENIX) {
      sb = "upsert into " + tableName + "(id,total_dataset,rule_size,antecedent,consequent,antecedent_size," +
        "antecedent_instance_size,antecedent_support,consequent_size,consequent_instance_size,consequent_support," +
        "rule_support,confidence,lift,deployment_capability,creation_date,version," + "\"key\"" +
        "\"" + defaultPkName + "\") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    } else if (tDataSource.getType eq DataBaseType.MYSQL) {
      sb = "insert into " + tableName + "(" +
        //  "id," +   // ID 列不插入到数据表，主键，自增
        "total_dataset," + //
        "rule_size," +
        "antecedent," +
        "consequent," +
        "antecedent_size," +
        "antecedent_instance_size," +
        "antecedent_support," +
        "consequent_size," +
        "consequent_instance_size," +
        "consequent_support," +
        "rule_support," +
        "confidence," +
        "lift," +
        "deployment_capability," +
        "creation_date," +
        "version," +
        "`key`" +
        ") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    } else {
      throw new Exception("关联规则落地不支持该类型的数据库")
    }
    return sb
  }

  
}
