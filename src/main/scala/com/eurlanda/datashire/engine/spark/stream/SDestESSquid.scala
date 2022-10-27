package com.eurlanda.datashire.engine.spark.stream

import java.util

import com.eurlanda.datashire.engine.entity._
import com.eurlanda.datashire.engine.spark.util.ESSquidUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}

/**
  * Created by zhudebin on 16/1/18.
  */
class SDestESSquid(path: String,
                   es_nodes:String,
                   es_port: String,
                   id2name:util.Map[Integer, String],
                   is_mapping_id:String) extends SSquid {

  def run(sc:StreamingContext): Unit = {

    val saveFun = (rdd: RDD[java.util.Map[Integer, DataCell]], time: Time) => {
      ESSquidUtil.saveToES(rdd.sparkContext, path, es_nodes, es_port, rdd, id2name, is_mapping_id)
    }
    preSquids(0).asInstanceOf[SSquid].outDStream.foreachRDD(saveFun)
  }
  @transient
  override var preSquids: List[ESquid] = _
  @transient
  override var outDStream: DStream[util.Map[Integer, DataCell]] = _
}
