package com.eurlanda.datashire.engine.spark.stream

import java.util

import com.eurlanda.datashire.engine.entity.{AggregateAction, DataCell, ESquid}
import com.eurlanda.datashire.engine.spark.util.ScalaMethodUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by zhudebin on 16/3/14.
  */
class SAggregateSquid(groupKeyList: util.List[Integer], aaList: util.List[AggregateAction]) extends SSquid {
  override var preSquids: List[ESquid] = _

  override def run(sc: StreamingContext): Any = {
    outDStream = preSquids(0).asInstanceOf[SSquid].outDStream.transform(rdd => {
//      SquidUtil.aggregateRDD(rdd.toJavaRDD(), groupKeyList, aaList).rdd
      ScalaMethodUtil.aggregateRDD(rdd.toJavaRDD(), groupKeyList, aaList)
    })
  }

  override var outDStream: DStream[util.Map[Integer, DataCell]] = _
}
