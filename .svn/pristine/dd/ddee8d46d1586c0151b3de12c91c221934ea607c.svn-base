package com.eurlanda.datashire.engine.spark.stream

import java.util

import com.eurlanda.datashire.engine.entity.{DataCell, ESquid, TFilterExpression}
import com.eurlanda.datashire.engine.util.SquidUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by zhudebin on 16/3/14.
  */
class SFilterSquid(filterExpression: TFilterExpression) extends SSquid {
  override var preSquids: List[ESquid] = _

  override def run(sc: StreamingContext): Any = {
    outDStream = preSquids(0).asInstanceOf[SSquid].outDStream.transform(rdd => {
      SquidUtil.filterRDD(rdd.toJavaRDD(), filterExpression).rdd
    })
  }

  override var outDStream: DStream[util.Map[Integer, DataCell]] = _
}
