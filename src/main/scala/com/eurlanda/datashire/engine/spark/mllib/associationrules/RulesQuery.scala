package com.eurlanda.datashire.engine.spark.mllib.associationrules

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import com.eurlanda.datashire.engine.util.{ConfigurationUtil}
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * 查询关联规则
  */
class RulesQuery {

  private val splitFlag = "," // 项之间的分隔符
  private val log: Log = LogFactory.getLog(classOf[RulesQuery])

  var antecedent: String = null
  var antecedentSize: Int = 0
  var consequent: String= null
  var consequentSize: Int=0
  var minConfidence: Double =0.0
  var minRuleSupport: Double =0.0
  var minLift: Double = 0.0
  var ruleSize: Int = 0
  var modelVersion  = -1  // -1 表示最大版本
  var key: String = null

  /**
    * 从数据库中直接查询所有满足条件的规则
    *
    * @param tableName
    * @return 返回CSV格式，表中一行数据组成一个CSV
    */
  def rulesQueryCSVFromDB(tableName: String): Array[String] = {
    val res = new ArrayBuffer[String]()
    var connection : Connection = null
    var statement : Statement = null
    var resultSet : ResultSet = null

    val url = "jdbc:mysql://"+ ConfigurationUtil.getInnerDataMiningMySQLHost+":" +
      ConfigurationUtil.getInnerDataMiningMySQLPort +"/" +
      ConfigurationUtil.getInnerDataMiningMySQLDatabaseName

    try {
      Class.forName("com.mysql.jdbc.Driver")
      connection = DriverManager.getConnection(url, ConfigurationUtil.getInnerDataMiningMySQLUserName,
                                               ConfigurationUtil.getInnerDataMiningMySQLUserPassword)
      val sql = getQuerySql(tableName)
      log.debug(sql)
      statement = connection.createStatement
      resultSet = statement.executeQuery(sql)
      while (resultSet.next()) {
        val id = resultSet.getLong("id")
        val antecedentres = resultSet.getString("antecedent")
        val consequentres = resultSet.getString("consequent")
        val antecedent_size = resultSet.getInt("antecedent_size")
        val antecedent_instance_size = resultSet.getInt("antecedent_instance_size")
        val antecedent_support =  resultSet.getDouble("antecedent_support")
        val consequent_size = resultSet.getInt("consequent_size")
        val consequent_instance_size = resultSet.getInt("consequent_instance_size")
        val consequent_support = resultSet.getDouble("consequent_support")
        val rule_support =  resultSet.getDouble("rule_support")
        val confidence = resultSet.getDouble("confidence")
        val lift =  resultSet.getDouble("lift")
        val deployment_capability =  resultSet.getDouble("deployment_capability")

        //组装成csv格式
        val csv = id + "," +
          antecedentres + "," +
          antecedent_size + "," +
          antecedent_instance_size + "," +
          antecedent_support + "," +
          consequentres + "," +
          consequent_size + "," +
          consequent_instance_size + "," +
          consequent_support + "," +
          confidence + "," +
          rule_support + "," +
          lift + "," +
          deployment_capability

        if (antecedent == null && consequent == null) {
          //  前项 后项 不参入条件
          res.append(csv)
        } else if (antecedent != null && !antecedent.trim.isEmpty && consequent == null) {
          //  前项参入条件，后项不参入条件
          val antecedentItems = antecedentres.toString.split(splitFlag)
          if (isArrayElementsEqual(antecedentItems, antecedent.split(splitFlag))) res.append(csv)
        } else if (antecedent == null && consequent != null && !consequent.trim.isEmpty) {
          // 前项不参入条件，后项参入条件
          val consequentItems = consequentres.toString.split(splitFlag)
          if (isArrayElementsEqual(consequentItems, consequent.split(splitFlag))) res.append(csv)
        } else {
          // 查询前项参入条件，后项参入条件
          val antecedentItems = antecedentres.toString.split(splitFlag)
          val consequentItems = consequentres.toString.split(splitFlag)
          if (isArrayElementsEqual(antecedentItems, antecedent.split(splitFlag)) &&
            isArrayElementsEqual(consequentItems, consequent.split(splitFlag))) res.append(csv)
        }
      }
      res.toArray
    } catch {
      case e: Throwable => {
        val errMsg = e.getMessage
        log.error("查询关联规则错误，" + errMsg)
        log.error(e.getCause)
        log.error(e.getStackTrace)
        throw new RuntimeException("查询关联规则错误，" + errMsg,e)
      }
    }finally {
      if(resultSet!= null){
        resultSet.close()
      }
      if(statement!= null){
        statement.close()
      }
      if(connection!= null){
        connection.close()
      }
    }
  }

  private def getQuerySql(tableName:String ):String = {
    var sql = "SELECT * FROM " + tableName +
      " WHERE confidence >= " + minConfidence +
      " AND rule_support >= " + minRuleSupport +
      " AND lift >= " + minLift + " "
    if (antecedentSize > 0) {
      sql += " AND antecedent_size = " + antecedentSize
    }
    if (consequentSize > 0) {
      sql += " AND consequent_size = " + consequentSize
    }
    sql += " AND version = " + modelVersion
    if (ruleSize >= 0) {
      sql += " limit " + ruleSize
    }
    return sql
  }

  /**
    * arr1 与 arr2的元素是否相等, 不计元素的顺序
    * @param arr1
    * @param arr2
    * @return
    */
  private def isArrayElementsEqual(arr1:Array[String],arr2:Array[String]):Boolean = {
    if(arr1.length != arr2.length){
      return false
    }
    for (emt2 <- arr2 if (!arr1.contains(emt2))) {
      return false
    }
    return true
  }

}
