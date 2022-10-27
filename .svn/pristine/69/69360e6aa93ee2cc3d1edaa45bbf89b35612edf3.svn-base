package com.eurlanda.datashire.engine.spark.stream

import java.util.{Map => JMap}

import com.eurlanda.datashire.engine.entity._
import com.eurlanda.datashire.engine.spark.util.ImpalaSquidUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}

/**
  * 流式落地impala
  * @param dataSource
  * @param columns
  */
class SDestImpalaSquid(dataSource: TDataSource, columns: java.util.Set[TColumn]) extends SSquid {

  def run(sc:StreamingContext): Unit = {


    val saveFun = (rdd: RDD[java.util.Map[Integer, DataCell]], time: Time) => {
      val params: JMap[String, Any] = new java.util.HashMap[String, Any]

      params.put(ImpalaSquidUtil.DATASOURCE, dataSource)
      params.put(ImpalaSquidUtil.TCOLUMNS, columns)

      ImpalaSquidUtil.saveToImpala(rdd.sparkContext, rdd.toJavaRDD(), params)
    }
    preSquids(0).asInstanceOf[SSquid].outDStream.foreachRDD(saveFun)
  }
  @transient
  override var preSquids: List[ESquid] = _
  @transient
  override var outDStream: DStream[JMap[Integer, DataCell]] = _
}
