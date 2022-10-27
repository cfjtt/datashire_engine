package com.eurlanda.datashire.engine.spark.stream

import java.util
import java.util.{List => JList, Map => JMap}

import com.eurlanda.datashire.engine.entity.{TStructField, DataCell, ESquid, TSquid}
import com.eurlanda.datashire.engine.spark.util.JoinUtil
import com.eurlanda.datashire.enumeration.JoinType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, TransformedDStreamUtil}

import scala.collection.JavaConversions._

/**
  * Created by zhudebin on 16/3/14.
  */
class SJoinSquid(joinSquids:JList[ESquid],
                 joinTypes: JList[JoinType],
                 joinExpressions: JList[String],
                 squidNames: JList[String],
                 id2Columns: JList[JMap[Integer, TStructField]]) extends SSquid {
  override var preSquids: List[ESquid] = joinSquids.toList

  override def run(sc: StreamingContext): Any = {
    // 验证第一个 是否为 SSquid,不是则数据异常
    assert(joinSquids.get(0).isInstanceOf[SSquid], "流式Join操作,第一个必须为流式squid")
    import scala.collection.JavaConversions._

    val dStreams = joinSquids.toList.filter(_.isInstanceOf[SSquid]).map(eSquid => {
      eSquid.asInstanceOf[SSquid].outDStream
    })

    // 排列好Join顺序
    val rdds = joinSquids.toList.filter(_.isInstanceOf[TSquid]).map(eSquid => {
      eSquid.asInstanceOf[TSquid].getOutRDD
    })

    val rddTypes = joinSquids.toList.map(es => {
      if(es.isInstanceOf[SSquid]) {
        1
      } else {
        0
      }
    })

    outDStream = TransformedDStreamUtil.transform[JMap[Integer,DataCell]](dStreams, (seq, t) => {
      val firstDstreamRDD:RDD[JMap[Integer, DataCell]] = seq.get(0).asInstanceOf[RDD[JMap[Integer, DataCell]]]
      // Get the singleton instance of SQLContext
      val sqlCtx = SQLContext.getOrCreate(firstDstreamRDD.sparkContext)

      import scala.collection.JavaConversions._

      val sortedRDDs: JList[RDD[JMap[Integer, DataCell]]] = new util.ArrayList[RDD[JMap[Integer, DataCell]]]()
      sortedRDDs.add(firstDstreamRDD)

      var rddLen = 0
      for(i <- 1 until rddTypes.size) {
        if(rddTypes.get(i) == 1) {
          // stream
          sortedRDDs.add(seq.get(i-rddLen).asInstanceOf[RDD[JMap[Integer, DataCell]]])
        } else {
          // rdd
          sortedRDDs.add(rdds.get(rddLen))
          rddLen += 1
        }
      }

      JoinUtil.joinRDD(sqlCtx.sparkSession, joinTypes, joinExpressions, sortedRDDs, squidNames, id2Columns)
    })

  }

  override var outDStream: DStream[util.Map[Integer, DataCell]] = _
}
