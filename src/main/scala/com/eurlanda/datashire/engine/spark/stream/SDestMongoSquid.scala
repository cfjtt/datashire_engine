package com.eurlanda.datashire.engine.spark.stream

import java.util.{Map => JMap}

import com.eurlanda.datashire.engine.entity._
import com.eurlanda.datashire.engine.spark.util.MongoSquidUtil
import org.apache.spark.CustomJavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}

/**
  * 流式落地Mongodb
  *
  * @param dataSource
  * @param columns
  */
class SDestMongoSquid(dataSource : TNoSQLDataSource,
                      columns: java.util.Set[TColumn],
                      truncateExistingData: Boolean) extends SSquid {

  def run(sc:StreamingContext): Unit = {


    val saveFun = (rdd: RDD[java.util.Map[Integer, DataCell]], time: Time) => {
      MongoSquidUtil.saveToMongo(dataSource, columns, rdd, new CustomJavaSparkContext(rdd.sparkContext), truncateExistingData)
    }
    preSquids(0).asInstanceOf[SSquid].outDStream.foreachRDD(saveFun)
  }
  @transient
  override var preSquids: List[ESquid] = _
  @transient
  override var outDStream: DStream[JMap[Integer, DataCell]] = _
}
