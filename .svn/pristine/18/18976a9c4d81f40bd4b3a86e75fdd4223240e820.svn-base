package com.eurlanda.datashire.engine.spark

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.{List => JList}

import com.eurlanda.datashire.enumeration.DataBaseType
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zhudebin on 15-7-23.
 */
object SplitJdbcRDDTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[4]")
      .setAppName("SplitJdbcRDDTest")
    val sc = new SparkContext(conf)

    val pageCompute = (pstmt:PreparedStatement,params:JList[AnyRef]) => {
      if(params != null && params.size()>0) {

        for(i <- 0 until params.size()) {
          pstmt.setObject(i+1, params.get(i))
        }
      }
    }

    val mapRow = (rs:ResultSet) => {
      rs.getString(1)
    }

    val params = new java.util.ArrayList[Object]()
    params.add(new Integer(2))

    val rdd = new SplitJdbcRDD[Object](sc, getConnection,
      "select * from ed_sina_weibo", DataBaseType.MYSQL,
      "ed_sina_weibo", "alias",
      "come_from",
      DbBaseDatatype.NVARCHAR,
      "id > ?",
      10, pageCompute, mapRow, params)
    val count = rdd.count()
    println(count)
  }

  def getConnection(): Connection = {
    classOf[com.mysql.jdbc.Driver]
    DriverManager.getConnection("jdbc:mysql://192.168.137.4:3306/dap", "root", "root")
  }

}
