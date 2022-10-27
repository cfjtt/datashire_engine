package org.apache.spark.deploy.yarn

import com.eurlanda.datashire.engine.entity.{TColumn, TDataSource, TDataType}
import com.eurlanda.datashire.engine.spark.stream.{SDataFallSquid, SKafkaSquid, SSquidFlow}
import com.eurlanda.datashire.engine.util.ConfigurationUtil
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.util.Random

/**
  * Created by zhudebin on 16/1/18.
  */
object StreamJobTest2 {

  /**
    * 第一个参数  squidflowId
    * 第二个参数  流式计算batch间隔 单位毫秒(推荐500 ~ 2000)
    * @param args
    */
  def main(args: Array[String]) {
    /*
    if(args.length < 2) {
      println(" 请输入两个参数 <squidflowId> <间隔> ")
      return
    }*/

    val squidFlowId = 363
    val batchDuration = 200l


    var squidName:String = "eurlanda"
    /**
    // 查询 流式 squidflow
    val dao: SSquidFlowDao = ConstantUtil.getBean(classOf[SSquidFlowDao])
    val squidFlow: SquidFlow = dao.getSSquidFlow(squidFlowId)

    val squidFlowDao: SquidFlowDao = ConstantUtil.getBean(classOf[SquidFlowDao])
    val infoMap: java.util.Map[String, AnyRef] = squidFlowDao.getSquidFlowInfo(squidFlow.getId)

    // 翻译
    val ss = new StreamBuilderContext(squidFlow).build()
      */

    // 运行
    val ss = genSSquidFlow()

//    squidName = squidFlow.getName
    val sc = new StreamingContext(new SparkConf()
      .setMaster("local[4]")
      .setAppName("数猎_" +  squidName),
      Milliseconds(batchDuration))

    ss.run(sc)

    sc.start()
    sc.awaitTermination()

  }

  def genSSquidFlow(): SSquidFlow = {
    val columnSet = new java.util.HashSet[TColumn]()
    columnSet.add(new TColumn(TDataType.STRING, "KEY", 1))
    columnSet.add(new TColumn(TDataType.STRING, "VALUE", 2))

    val k:SKafkaSquid = new SKafkaSquid("e101:2181", "ds_log"+new Random().nextInt(100),
      Map("ds_log" -> 1), StorageLevel.MEMORY_AND_DISK_2, columnSet.toList)

//    val dataSource = new TDataSource("192.168.137.17", 3306, "test_db", "111111", "root", "test_2", DataBaseType.MYSQL)
    val tDataSource = new TDataSource()
    tDataSource.setType(DataBaseType.HBASE_PHOENIX)
    tDataSource.setTableName("KAFKA_STREAM_TEST")
    tDataSource.setHost(ConfigurationUtil.getInnerHbaseHost + ":" + ConfigurationUtil.getInnerHbasePort)
    tDataSource.setCDC(false)


    val f:SDataFallSquid = new SDataFallSquid(tDataSource, columnSet)

    f.preSquids = List(k)
    val ss: SSquidFlow = new SSquidFlow(List(k, f), "")
    ss
  }
}
