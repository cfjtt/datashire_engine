package com.eurlanda.datashire.engine.spark.stream

import java.util

import com.eurlanda.datashire.engine.entity.{DataCell, ESquid, TColumn, TDataType}
import com.eurlanda.datashire.engine.util.DateUtil
import com.eurlanda.datashire.server.utils.Constants
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by zhudebin on 16/1/18.
  */
class SKafkaSquid(zkQuorum:String, groupId:String,
                  topics:Map[String, Int],
                  storageLevel:StorageLevel = StorageLevel.MEMORY_AND_DISK_2,
                  columnSet : List[TColumn]) extends SSquid {
  @transient
  var outDStream: DStream[java.util.Map[Integer, DataCell]] = _

  def run(sc: StreamingContext) {
    // key
    var t1:Int = -1
    // value
    var t2:Int = -1
    // create_date
    var t3:Int = -1

    val extraction_date_str = Constants.DEFAULT_EXTRACT_COLUMN_NAME.toUpperCase

    columnSet.map(c => {
      c.getName.toUpperCase.trim match {
        case "KEY" => t1 = c.getId
        case "VALUE" => t2 = c.getId
        case extraction_date_str => t3 = c.getId
      }
    })

    val ids = (t1, t2, t3)

    outDStream = KafkaUtils.createStream(sc, zkQuorum, groupId, topics, storageLevel).map(t2 => {
      val map = new util.HashMap[Integer, DataCell]()
      if(ids._1 > 0) {
        map.put(ids._1, new DataCell(TDataType.STRING, t2._1))
      }
      if(ids._2 > 0) {
        map.put(ids._2, new DataCell(TDataType.STRING, t2._2))
      }
      if(ids._3 > 0) {
        map.put(ids._3, new DataCell(TDataType.TIMESTAMP, DateUtil.nowTimestamp()))
      }
      map
    })
  }
  @transient
  override var preSquids: List[ESquid] = _
}
