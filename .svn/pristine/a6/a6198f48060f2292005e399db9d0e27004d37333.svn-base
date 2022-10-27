package com.eurlanda.datashire.engine.spark.stream

import java.util
import java.util.{Map => JMap}

import com.eurlanda.datashire.engine.entity.{DataCell, ESquid, TUnionType}
import com.eurlanda.datashire.engine.util.SquidUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by zhudebin on 16/3/15.
  */
class SUnionSquid(unionType :TUnionType,
                  leftSquid:SSquid,
                  rightSquid:SSquid,
                  leftInKeyList:util.List[Integer],
                  rightInKeyList:util.List[Integer]) extends SSquid{
  override var preSquids: List[ESquid] = List(leftSquid, rightSquid)

  override def run(sc: StreamingContext): Any = {
    require(preSquids.size == 2, "union 操作必须含有两个squid")
    val leftSSquid = preSquids(0).asInstanceOf[SSquid]
    val rightSSquid:SSquid = preSquids(1).asInstanceOf[SSquid]

    import scala.collection.convert.wrapAsJava.seqAsJavaList

    outDStream = leftSSquid.outDStream
      .transformWith[util.Map[Integer, DataCell], util.Map[Integer, DataCell]](
      rightSSquid.outDStream,
      (rdd1:RDD[util.Map[Integer, DataCell]], rdd2:RDD[util.Map[Integer, DataCell]]) => {
        SquidUtil.unionRDD(seqAsJavaList(Seq(rdd1.toJavaRDD(), rdd2.toJavaRDD())), leftInKeyList, rightInKeyList, unionType).rdd
    })


    outDStream
  }

  override var outDStream: DStream[util.Map[Integer, DataCell]] = _
}
