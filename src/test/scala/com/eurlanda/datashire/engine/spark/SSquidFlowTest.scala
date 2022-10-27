package com.eurlanda.datashire.engine.spark

import com.eurlanda.datashire.engine.entity.{TColumn, TDataSource, TDataType}
import com.eurlanda.datashire.engine.spark.stream.{SDataFallSquid, SKafkaSquid, SSquidFlow}
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by zhudebin on 16/1/18.
  */
object SSquidFlowTest {

  def main(args: Array[String]) {

    val k:SKafkaSquid = new SKafkaSquid("e101:2181", "ds_log",
      Map("ds_log" -> 1), storageLevel = StorageLevel.MEMORY_AND_DISK_2,
      List(new TColumn("c1", 1, TDataType.STRING), new TColumn("c2", 2, TDataType.STRING)))

    val dataSource = new TDataSource("192.168.137.17", 3306, "test_db", "111111", "root", "test_2", DataBaseType.MYSQL)
    val columnSet = new java.util.HashSet[TColumn]()
    columnSet.add(new TColumn(TDataType.STRING, "c1", 1))
    columnSet.add(new TColumn(TDataType.STRING, "c2", 2))

    val f:SDataFallSquid = new SDataFallSquid(dataSource, columnSet)

    f.preSquids = List(k)
    val ss: SSquidFlow = new SSquidFlow(List(k, f), "hello world")


    val sc = new StreamingContext(new SparkConf()
      .setMaster("local[4]")
      .setAppName("squidflow name"),
      Seconds(1))

    ss.run(sc)

    sc.start()
    sc.awaitTermination()
  }


}
