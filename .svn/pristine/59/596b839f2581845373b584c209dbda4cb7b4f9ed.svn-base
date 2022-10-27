package org.apache.spark.deploy.yarn

import com.eurlanda.datashire.engine.dao.{SSquidFlowDao, SquidFlowDao}
import com.eurlanda.datashire.engine.entity.{TColumn, TDataSource, TDataType}
import com.eurlanda.datashire.engine.spark.stream.{SDataFallSquid, SKafkaSquid, SSquidFlow}
import com.eurlanda.datashire.engine.spark.translation.StreamBuilderContext
import com.eurlanda.datashire.engine.util.ConstantUtil
import com.eurlanda.datashire.entity.SquidFlow
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.util.Random

/**
  * 流式本地运行测试启动类
  *
  * Created by zhudebin on 16/1/18.
  */
object StreamJobLocalTest {

  /**
    * 第一个参数  squidflowId
    * 第二个参数  流式计算batch间隔 单位毫秒(推荐500 ~ 2000)
    * @param args
    */
  def main(args: Array[String]) {
    runStreamJobLocal(308)
  }

  def runStreamJobLocal(squidFlowId:Int) {
    val batchDuration = 2000l


    var squidFlowName:String = "eurlanda"
    // 查询 流式 squidflow
    val dao: SSquidFlowDao = ConstantUtil.getSSquidFlowDao()
    val squidFlow: SquidFlow = dao.getSSquidFlow(squidFlowId)

    val squidFlowDao: SquidFlowDao = ConstantUtil.getSquidFlowDao()
    val infoMap: java.util.Map[String, AnyRef] = squidFlowDao.getSquidFlowInfo(squidFlow.getId)

    // 翻译
    val ss = new StreamBuilderContext(squidFlow).build()

    // 运行
    //    val ss = genSSquidFlow()

    squidFlowName = squidFlow.getName
    val sc = new StreamingContext(new SparkConf()
//      .setMaster("local-cluster[4, 1, 1200]")
      .setMaster("local[4]")
      .setSparkHome("/Users/zhudebin/soft/ds/spark-2.0/spark-2.0.1-ds-bin-ds-spark")
      .setAppName("数猎_" + infoMap.get("REPOSITORYNAME") +
        "_" + infoMap.get("PROJECTNAME") + "_" +  squidFlowName),
      Milliseconds(batchDuration))

    ss.run(sc)

    sc.start()
    sc.awaitTermination()
  }

  def genSSquidFlow(): SSquidFlow = {
    val columnSet = new java.util.HashSet[TColumn]()
    columnSet.add(new TColumn(TDataType.STRING, "c1", 1))
    columnSet.add(new TColumn(TDataType.STRING, "c2", 2))

    val k:SKafkaSquid = new SKafkaSquid("e101:2181", "ds_log"+new Random().nextInt(100),
      Map("ds_log" -> 1), StorageLevel.MEMORY_AND_DISK_2, columnSet.toList)

    val dataSource = new TDataSource("192.168.137.17", 3306, "test_db", "111111", "root", "test_2", DataBaseType.MYSQL)


    val f:SDataFallSquid = new SDataFallSquid(dataSource, columnSet)

    f.preSquids = List(k)
    val ss: SSquidFlow = new SSquidFlow(List(k, f), "")
    ss
  }
}
