package com.eurlanda.datashire.engine.entity

import java.util
import java.util.Map

import org.apache.commons.logging.LogFactory
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}

/**
  * Created by Administrator on 2017-08-02.
  * 回归系数
  * 四列 i,j,value,groupkey,modelVersion
  */
class TCoefficientSquid extends TSquid {

  private val log = LogFactory.getLog(classOf[TCoefficientSquid])
  setType(TSquidType.COEFFICIENT_SQUID)

  protected var preSquid: TSquid = null
  var tColumns : java.util.List[TColumn] = null
  var squidName: String = null

  override def run(jsc: JavaSparkContext): Object = {

    log.info("翻译 TCoefficientSquid,Squid: " + getSquidId)

    if (preSquid.outDataFrame == null) {
      preSquid.runSquid(jsc)
    }
    if (preSquid.outDataFrame != null) {
      this.outDataFrame = preSquid.outDataFrame
      import com.eurlanda.datashire.engine.spark.squid.TJoinSquid
      val id2TStructField = TDataCatchSquid.getId2TStructField(tColumns)
      val tmpRDD = TJoinSquid.dataFrameToRDD(outDataFrame, id2TStructField)
      this.outRDD = TDataCatchSquid.removeSquareBrackets(tmpRDD)
      this.outRDD
    } else if (preSquid.getOutRDD != null) {
      this.outRDD = preSquid.outRDD
      this.outRDD
    } else {
      throw new RuntimeException("TCoefficientSquid 的前一个Squid" + preSquid.getName + "的数据是null")
    }
  }

}
