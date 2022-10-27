package com.eurlanda.datashire.engine.spark.stream

import java.util
import java.util.{List => JList, Map => JMap}

import com.eurlanda.datashire.engine.entity._
import com.eurlanda.datashire.engine.spark.util.HdfsSquidUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}

/**
  * Created by zhudebin on 16/1/18.
  */
class SDestHDFSSquid(fallColumnNames: Array[String] , fallColumnIds: Array[Integer],
                     partitionColumnIds: Array[Integer],params : java.util.Map[String,Object]) extends SSquid {

  def run(sc:StreamingContext): Unit = {

    val saveFun = (rdd: RDD[java.util.Map[Integer, DataCell]], time: Time) => {
    //  HdfsSquidUtil.saveToHdfs(rdd.sparkContext, rdd.toJavaRDD(), idList, params, fileType)
      HdfsSquidUtil.saveToHdfsByPartitions(rdd.sparkContext, rdd.toJavaRDD(), fallColumnNames, fallColumnIds, partitionColumnIds , params)
    }
    preSquids(0).asInstanceOf[SSquid].outDStream.foreachRDD(saveFun)
  }
  @transient
  override var preSquids: List[ESquid] = _
  @transient
  override var outDStream: DStream[util.Map[Integer, DataCell]] = _
}
