package com.eurlanda.datashire.engine.spark.stream

import java.util

import com.eurlanda.datashire.engine.entity.{DataCell, ESquid}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * 为dStream 创建 window
  * @param windowDuration 单位毫秒数, 将多少毫秒内的数据整合到一个window中, 必须为 batch interval的整数倍
  * @param slideDuration 单位毫秒数, 将一个window 内的数据,多少毫秒数切分为一个RDD, 必须为 batch interval的整数倍
  */
class SWindowSquid(windowDuration:Long, slideDuration: Long = 0) extends SSquid {
  override var preSquids: List[ESquid] = _

  override def run(sc: StreamingContext): Any = {
    outDStream = if(slideDuration == 0) {
      preSquids(0).asInstanceOf[SSquid].outDStream.window(Milliseconds(windowDuration))
    } else {
      preSquids(0).asInstanceOf[SSquid].outDStream.window(Milliseconds(windowDuration), Milliseconds(slideDuration))

    }
  }

  override var outDStream: DStream[util.Map[Integer, DataCell]] = _
}
