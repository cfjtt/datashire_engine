package com.eurlanda.datashire.engine.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by zhudebin on 16/1/18.
  */
object SSquidFlowTest2 {

  def main(args: Array[String]) {

    /**
    val k:SKafkaSquid = new SKafkaSquid("e101:2181", "ds_log", Map("" -> 1), ids=(1,2))

    val dataSource = new TDataSource("192.168.137.17", 3306, "test_db", "123456", "root", "test_2", DataBaseType.MYSQL)
    val columnSet = new java.util.HashSet[TColumn]()
    columnSet.add(new TColumn(TDataType.STRING, "c1", 1))
    columnSet.add(new TColumn(TDataType.STRING, "c2", 2))

    val f:SDataFallSquid = new SDataFallSquid(dataSource, columnSet)

    f.preSquids = List(k)
    val ss: SSquidFlow = new SSquidFlow(List(k, f))
      */

    val conf = new SparkConf().setAppName("Streaming test1")
      .setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(4))

    val lines = ssc.socketTextStream("192.168.137.111", 9999)


    lines.print()
  }


}
