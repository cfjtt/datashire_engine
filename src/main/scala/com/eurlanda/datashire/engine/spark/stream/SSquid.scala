package com.eurlanda.datashire.engine.spark.stream

import com.eurlanda.datashire.engine.entity.{DataCell, ESquid, TSquid}
import com.eurlanda.datashire.engine.spark.SquidStatus
import org.apache.spark.CustomJavaSparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer

/**
  * Created by zhudebin on 16/1/18.
  */
abstract class SSquid(var status: Int = SquidStatus.NOTSTART) extends ESquid with Serializable {

  @transient
  var preSquids: List[ESquid]
  @transient
  var outDStream: DStream[java.util.Map[Integer, DataCell]]
  @transient
  var dependenceSquids: ListBuffer[ESquid] = new ListBuffer[ESquid]

  def run(sc:StreamingContext):Any

  def runSquid(sc:StreamingContext):Any = {
    if(preSquids != null) {
      preSquids.foreach(s => {
        runESquid(s)
      })
    }

    if(dependenceSquids != null) {
      dependenceSquids.foreach(s => {
        runESquid(s)
      })
    }

    def runESquid(eSquid: ESquid) {
      if(eSquid.isInstanceOf[SSquid]) {
        if(eSquid.asInstanceOf[SSquid].outDStream == null
          && eSquid.asInstanceOf[SSquid].status == SquidStatus.NOTSTART) {
          eSquid.asInstanceOf[SSquid].runSquid(sc)
        }
      } else {
        // 批处理 TODO  未验证
        val ts = eSquid.asInstanceOf[TSquid]
        if(ts.getOutRDD == null) {
          ts.runSquid(new CustomJavaSparkContext(sc.sparkContext))
        }
      }
    }

    run(sc)
    status = SquidStatus.SUCCESS
  }

  def addDependenceSquid(eSquid: ESquid) {
    dependenceSquids.append(eSquid)
  }
}
