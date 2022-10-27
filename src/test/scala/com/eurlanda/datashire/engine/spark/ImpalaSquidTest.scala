package com.eurlanda.datashire.engine.spark

import java.util.{List => JList, Map => JMap}

import com.eurlanda.datashire.engine.entity.{DataCell, TColumn, TDataSource, TDataType}
import com.eurlanda.datashire.engine.spark.util.ImpalaSquidUtil
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by zhudebin on 16/5/17.
  */
object ImpalaSquidTest {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[6]")
      .setAppName("hdfs test")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val preRDD:JavaRDD[JMap[Integer, DataCell]] = sc.parallelize(1 to 100, 3).map(i => {
      val map:JMap[Integer, DataCell] = new java.util.HashMap[Integer, DataCell]()
      // id 1,2,3,4,5
      map.put(1, new DataCell(TDataType.STRING, "name_"+i))
      map.put(2, new DataCell(TDataType.INT, i))
      map.put(3, new DataCell(TDataType.STRING, "c3_"+i))
      map.put(4, new DataCell(TDataType.STRING, "c4_"+i))
      map.put(5, new DataCell(TDataType.STRING, "c5_"+i))
      map
    })

    val d :com.cloudera.impala.core.ImpalaJDBCDriver = null

    val columns = new mutable.HashSet[TColumn]()
    columns += new TColumn("name", 1, TDataType.STRING, false)
    columns += new TColumn("age", 2, TDataType.INT, false)
    columns += new TColumn("c3", 3, TDataType.STRING, false)
    columns += new TColumn("c4", 4, TDataType.STRING, false)
    columns += new TColumn("c5", 5, TDataType.STRING, false)

    val datasource = new TDataSource()
    datasource.setHost("192.168.137.133:21050")
    datasource.setDbName("file_formats")
    datasource.setTableName("t_user_test")
    datasource.setType(DataBaseType.IMPALA)

    val params = new java.util.HashMap[String, Any]()
    params.put("datasource", datasource)
    params.put("tcolumns", columns.toSet)
    ImpalaSquidUtil.saveToImpala(sc, preRDD, params)

  }


}
