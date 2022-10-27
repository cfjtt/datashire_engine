package com.eurlanda.datashire.engine.spark.stream

import java.util

import com.eurlanda.datashire.common.util.HbaseUtil
import com.eurlanda.datashire.engine.entity._
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.spark.CustomJavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}

import scala.collection.JavaConversions._

/**
  * Created by zhudebin on 16/1/18.
  */
class SDataFallSquid(dataSource:TDataSource,
                     columnSet: java.util.Set[TColumn]) extends SSquid {

  def run(sc:StreamingContext): Unit = {

    // 判断是否为hbase, 如果为hbase 需要判断TColumn中是否存在主键,没有则需要添加 HbaseUtil.DEFAULT_PK_NAME
    if(dataSource.getType == DataBaseType.HBASE_PHOENIX) {
      if(!columnSet.exists(tc => tc.isPrimaryKey)) {
        val guidColumn: TColumn = new TColumn(TDataType.STRING, HbaseUtil.DEFAULT_PK_NAME, -1)
        guidColumn.setSourceColumn(false)
        guidColumn.setGuid(true)
        columnSet.add(guidColumn)
      }
    }

    val saveFun = (rdd: RDD[java.util.Map[Integer, DataCell]], time: Time) => {
      new CustomJavaSparkContext(rdd.sparkContext).fallDataRDD(dataSource,
        columnSet, rdd.toJavaRDD(), false)
    }
    preSquids(0).asInstanceOf[SSquid].outDStream.foreachRDD(saveFun)
  }
  @transient
  override var preSquids: List[ESquid] = _
  @transient
  override var outDStream: DStream[util.Map[Integer, DataCell]] = _
}
