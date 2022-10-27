package com.eurlanda.datashire.engine.entity

import java.util
import java.util.{HashMap, List, Map}

import org.apache.commons.logging.LogFactory
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql._

/**
  * Created by Administrator on 2017-06-26.
  *
  * 翻译
  *
  */
class TDataCatchSquid extends TSquid {

  private val log = LogFactory.getLog(classOf[TDataCatchSquid])
  setType(TSquidType.DATACATCH_SQUID)

  protected var preSquid: TSquid = null
  var tColumns : java.util.List[TColumn] = null
  var squidName: String = null

  override def run(jsc: JavaSparkContext): Object = {

    log.info("翻译 DATACATCH_SQUID")

    if (preSquid.outDataFrame == null) {
      preSquid.runSquid(jsc)
    }
    if (preSquid.outDataFrame != null) {
      this.outDataFrame = preSquid.outDataFrame
      import com.eurlanda.datashire.engine.spark.squid.TJoinSquid
      val id2TStructField =TDataCatchSquid. getId2TStructField(tColumns)
      val tmpRDD = TJoinSquid.dataFrameToRDD(outDataFrame,id2TStructField)
      this.outRDD = TDataCatchSquid.removeSquareBrackets(tmpRDD)
      this.outRDD
    } else if (preSquid.getOutRDD != null) {
      this.outRDD = preSquid.outRDD
      this.outRDD
    } else {
      throw new RuntimeException("TDataCatchSquid的前一个Squid"+preSquid.getName+"的数据是null")
    }
  }



}

object TDataCatchSquid{
  /**
    * 因是csn，要去掉每个元素的前后方括号
    * @param javaRdd
    * @return
    */
  def removeSquareBrackets(javaRdd: JavaRDD[Map[Integer, DataCell]]): JavaRDD[Map[Integer, DataCell]] = {
    javaRdd.rdd.mapPartitions(par => {
      par.map(row => {
        val itermap = row.entrySet().iterator()
        val resMap: util.Map[Integer, DataCell] = new util.HashMap[Integer, DataCell]
        while (itermap.hasNext) {
          val tmp = itermap.next()  //  Integer, DataCell
          tmp.getValue.setData(tmp.getValue.getData.toString.replace("[", "").replace("]", ""))  // 去掉每个元素的前后的方括号 [ ]
          resMap.put(tmp.getKey, tmp.getValue)
        }
        resMap
      })
    })
  }

  def getId2TStructField(tColumns: util.List[TColumn]): util.List[util.Map[Integer, TStructField]] = {
    val resMap = new util.HashMap[Integer, TStructField]
    import scala.collection.JavaConversions._
    for (tcol <- tColumns) {
      val field = new TStructField(tcol.getName,tcol.getData_type, tcol.isNullable, tcol.getPrecision, tcol.getScale)
      resMap.put(tcol.getId, field)
    }
    val res = new util.ArrayList[util.Map[Integer,TStructField]]
    res.add(resMap)
    res
  }


}


