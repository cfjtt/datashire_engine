package com.eurlanda.datashire.engine.spark.squid

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{HashMap, List => JList, Map => JMap}

import com.eurlanda.datashire.engine.entity._
import com.eurlanda.datashire.engine.spark.util.{EngineUtil, JoinUtil}
import com.eurlanda.datashire.enumeration.JoinType
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, MapperFeature, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Created by zhudebin on 15/10/29.
 */
class TJoinSquid(preTSquids:JList[TSquid],
                 joinTypes: JList[JoinType],
                 joinExpressions: JList[String],
                 squidNames: JList[String], id2Columns: JList[JMap[Integer, TStructField]]) extends TSquid {

  def run(jsc: JavaSparkContext): AnyRef = {

    val session = getJobContext.getSparkSession

    import scala.collection.JavaConversions._

    val rdds: JList[RDD[JMap[Integer, DataCell]]] = new util.ArrayList[RDD[JMap[Integer, DataCell]]]()

    for(tsquid <- preTSquids) {
      rdds.add(tsquid.getOutRDD.rdd)
    }

    this.outDataFrame = JoinUtil.joinRDDToDataFrame(session, joinTypes,
      joinExpressions, rdds, squidNames, id2Columns)

    this.outRDD = TJoinSquid.dataFrameToRDD(outDataFrame, id2Columns).toJavaRDD()

    // 以前采用 rdd join 的方式实现,现已改为 DataFrame
//    this.outRDD = JoinUtil.joinRDD(sqlCtx, joinTypes, joinExpressions, rdds, squidNames, id2Columns).toJavaRDD()

    this.outRDD
  }
}

object TJoinSquid {

  def rddToDataFrame(session: SparkSession, squidName: String, rdd: RDD[JMap[Integer, DataCell]], id2Columns: JMap[Integer, TStructField]): DataFrame = {
    val ids = id2Columns.keySet().toArray(Array[Integer]()).sorted.toSeq
    val preDF = session.createDataFrame(rdd.map(m => {
      val ids1 = ids
      val rowArray = collection.mutable.ArrayBuffer[Any]()
      for(id <- ids1) {
        val data = m.get(id)
        if(data != null) {
          rowArray += m.get(id).getData
        } else {
          rowArray += null
        }

      }
      Row.fromSeq(rowArray.toSeq)
    }), TJoinSquid.genStructType(id2Columns))
      .as(squidName)
    preDF
  }

  def dataFrameToRDD(dataFrame: DataFrame, id2Columns: JList[JMap[Integer, TStructField]]): RDD[JMap[Integer, DataCell]] = {
    dataFrame.rdd.map(row => {
      val m:JMap[Integer, DataCell] = new HashMap[Integer, DataCell]()
      // 封装到 datacell 中
      import scala.collection.JavaConversions._
      var idx:Int = 0
      for(i2c <- id2Columns) {
        val ids = i2c.keySet().toArray(Array[Integer]()).sorted.toSeq
        for(id <- ids) {
          m.put(id, new DataCell(i2c.get(id).getDataType, row(idx)))
          idx += 1
        }
      }
      m
    })
  }

  /**
    * id2columns中数据类型为TDataType
    *
    * @param dataFrame
    * @param id2Columns
    * @return
    */
  def dataFrameToRDDByTDataType(dataFrame: DataFrame, id2Columns: JList[JMap[Integer, TStructField]]): RDD[JMap[Integer, DataCell]] = {
    dataFrame.rdd.map(row => {
      val m:JMap[Integer, DataCell] = new HashMap[Integer, DataCell]()
      // 封装到 datacell 中
      import scala.collection.JavaConversions._
      var idx:Int = 0
      val rowSize = row.length
      for(i2c <- id2Columns) {
        val ids = i2c.keySet().toArray(Array[Integer]()).sorted.toSeq
        for(id <- ids) {
          if(idx < rowSize) {   // 如果存在该行数据
            m.put(id, new DataCell(i2c.get(id).getDataType, row(idx)))
          } else {              // 不存在,则放空
            m.put(id, new DataCell(i2c.get(id).getDataType, null))
          }
          idx += 1
        }
      }
      m
    })
  }

  def rddSpecialTypeToString(outRDD : JavaRDD[JMap[Integer, DataCell]], specialTypecolumn : JMap[Integer,TStructField]): JavaRDD[JMap[Integer, DataCell]]={
    outRDD.rdd.map(m => {
        val mapper = getJsonMapper()
        val ids = specialTypecolumn.keySet().toArray(Array[Integer]()).toSeq
        for(id <- ids){
          if (m.containsKey(id)){
            m.get(id).setData(mapper.writeValueAsString(m.get(id).getData))
          }
        }
        m
      }).toJavaRDD()
  }

  def dataFrameSpecialTypeToString(session : SparkSession,outDataFrame : DataFrame,specialTypeColumn : JMap[Integer,TStructField],extractId2column : JMap[Integer,TStructField],squidName : String) : DataFrame = {
    session.createDataFrame(outDataFrame.rdd.map(row => {
      val mapper = getJsonMapper()
      val rowArray = row.toSeq.toArray
      for(structField : TStructField <- specialTypeColumn.values().toArray(Array[TStructField]())){
        val index = row.fieldIndex(structField.getName)
        val value = mapper.writeValueAsString(row(index))
        rowArray.update(index,value)
      }
      Row.fromSeq(rowArray)
    }),TJoinSquid.genStructType(extractId2column)).as(squidName)
  }
  def groupTaggingDataFrameToRDD(dataFrame: DataFrame, id2Columns: JMap[Integer, TStructField]): RDD[JMap[Integer, DataCell]] = {
    val schema = dataFrame.schema
    dataFrame.rdd.map(row => {
      val m:JMap[Integer, DataCell] = new HashMap[Integer, DataCell]()
      // 封装到 datacell 中
      import scala.collection.JavaConversions._
      for(i2c <- id2Columns.entrySet()) {
        // 获取列名
        val columnName = i2c.getValue.getName
        m.put(i2c.getKey, new DataCell(
          i2c.getValue.getDataType,
          row(schema.fieldIndex(columnName))
        ))
      }
      m
    })
  }

  def genStructType(id2Column:JMap[Integer, TStructField]):StructType = {
    val ids = id2Column.keySet().toArray(Array[Integer]()).sorted.toSeq
    val st = StructType(ids.map(id => {
      val tsf = id2Column.get(id)
      StructField(tsf.getName, EngineUtil.tDataTypeToSparkSqlType(tsf), tsf.isNullable)
    }))
    st
  }

  def joinTypeToStr(joinType: JoinType):String = {
    joinType match {
      case JoinType.InnerJoin => "inner"
      case JoinType.LeftOuterJoin => "left_outer"
      case JoinType.RightOuterJoin => "right_outer"
      case JoinType.FullJoin => "outer"
      case JoinType.CrossJoin => "inner"
    }
  }

  def getJoinExpression(joinExpression:String, joinType: JoinType):String = {
    if(joinType == JoinType.CrossJoin) {
      "1=1"
    } else {
      joinExpression
    }
  }

  def getJsonMapper() : ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.getSerializerProvider.setDefaultKeySerializer(new JsonSerializer[Object] {
      override def serialize(t: Object, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = {
        if(t.isInstanceOf[Timestamp]){
          val dataFormat : SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val dataStr = dataFormat.format(t)
          jsonGenerator.writeFieldName(dataStr)
        }
      }
    })
    mapper.getSerializerProvider.setNullKeySerializer(new JsonSerializer[Object] {
      override def serialize(t: Object, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = {
        jsonGenerator.writeFieldName("null")
      }
    })
    mapper.registerModule(DefaultScalaModule)
    mapper
  }
}
