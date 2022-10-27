package com.eurlanda.datashire.engine.spark.stream

import com.eurlanda.datashire.engine.entity.ESquid
import org.apache.spark.streaming.StreamingContext

/**
  * Created by zhudebin on 16/1/18.
  */
class SSquidFlow(squids:List[ESquid], val squidFlowName: String) {

  def run(sc:StreamingContext): Unit = {
    squids.foreach(s => {
      if(s.isInstanceOf[SSquid]) {
        s.asInstanceOf[SSquid].runSquid(sc)
      }
    })
  }

}
