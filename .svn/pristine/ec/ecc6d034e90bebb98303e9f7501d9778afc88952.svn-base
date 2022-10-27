package com.eurlanda.datashire.engine.spark.util

import java.util.{List => JList, Map => JMap}

import com.eurlanda.datashire.engine.entity.{TStructField, DataCell}
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid
import com.eurlanda.datashire.enumeration.JoinType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column => SColumn, SparkSession, DataFrame, SQLContext, Utils}

/**
  * Created by zhudebin on 16/3/14.
  */
object JoinUtil {

  def joinRDDToDataFrame(session: SparkSession,
                         joinTypes: JList[JoinType],
                         joinExpressions: JList[String],
                         rdds: JList[RDD[JMap[Integer, DataCell]]],
                         squidNames: JList[String],
                         id2Columns: JList[JMap[Integer, TStructField]]):DataFrame = {
    var idx = 0
    import scala.collection.JavaConversions._
    var preDF:DataFrame = null
    for(rdd <- rdds.toList) {
      // join 的第一个表
      if(preDF == null) {

        preDF = TJoinSquid.rddToDataFrame(session, squidNames.get(idx), rdd, id2Columns.get(idx))

        /**
          * val ids = id2Columns.get(idx).keySet().toArray(Array[Integer]()).sorted.toSeq
          * preDF = sqlCtx.createDataFrame(rdd.map(m => {
          * val ids1 = ids
          * val rowArray = collection.mutable.ArrayBuffer[Any]()
          * for(id <- ids1) {
          * val data = m.get(id)
          * if(data != null) {
          * rowArray += m.get(id).getData
          * } else {
          * rowArray += null
          * }

          * }
          * Row.fromSeq(rowArray.toSeq)
          * }), TJoinSquid.genStructType(ids,id2Columns.get(idx)))
          * .as(squidNames.get(idx))
          */
      } else {  // join 后面的表

        val nextDF = TJoinSquid.rddToDataFrame(session, squidNames.get(idx), rdd, id2Columns.get(idx))
        /**
          * val ids = id2Columns.get(idx).keySet().toArray(Array[Integer]()).sorted.toSeq
          * val curDF = sqlCtx.createDataFrame(rdd.map(m => {
          * val ids2 = ids
          * val rowArray = collection.mutable.ArrayBuffer[Any]()
          * for(id <- ids2) {
          * val data = m.get(id)
          * if(data != null) {
          * rowArray += m.get(id).getData
          * } else {
          * rowArray += null
          * }

          * }
          * Row.fromSeq(rowArray.toSeq)
          * }), TJoinSquid.genStructType(ids,id2Columns.get(idx)))
          * .as(squidNames.get(idx))
          */

        // 与前面的dataFrame 进行join



        preDF = preDF.join(nextDF,
//          new SColumn(SqlParser.parseExpression(TJoinSquid.getJoinExpression(joinExpressions.get(idx), joinTypes.get(idx)))),
          new SColumn(Utils.sqlToExpression(
            TJoinSquid.getJoinExpression(joinExpressions.get(idx), joinTypes.get(idx)),
            session)),
          TJoinSquid.joinTypeToStr(joinTypes.get(idx)))
      }
      idx += 1
    }
    preDF
  }

  def joinRDD(session: SparkSession,
              joinTypes: JList[JoinType],
              joinExpressions: JList[String],
              rdds: JList[RDD[JMap[Integer, DataCell]]],
              squidNames: JList[String],
              id2Columns: JList[JMap[Integer, TStructField]]): RDD[JMap[Integer, DataCell]]= {

    val preDF = joinRDDToDataFrame(session, joinTypes, joinExpressions, rdds, squidNames, id2Columns)
    // dataframe to rdd
    TJoinSquid.dataFrameToRDD(preDF, id2Columns)

    /**
      * preDF.map(row => {
      * val m:JMap[Integer, DataCell] = new HashMap[Integer, DataCell]()
      * // 封装到 datacell 中

      * val i2cs = id2Columns

      * var idx:Int = 0
      * for(i2c <- i2cs) {
      * val ids = i2c.keySet().toArray(Array[Integer]()).sorted.toSeq
      * for(id <- ids) {
      * m.put(id, new DataCell(TDataType.sysType2TDataType(i2c.get(id)._2), row(idx)))
      * idx += 1
      * }
      * }
      * m
      * })
      */
  }

}
