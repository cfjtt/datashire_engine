package com.eurlanda.datashire.engine.spark.util

import com.eurlanda.datashire.engine.entity.{TStructField, TDataType}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, DataFrame, SparkSession}
import org.apache.spark.sql.types._

/**
 * Created by zhudebin on 15/10/28.
 */
object EngineUtil {

  /**
  def columnTypeToSqlType(dataType: Int): DataType = {

    val tdt = TDataType.sysType2TDataType(dataType)

    tDataTypeToSparkSqlType(tdt)
  }  */

  def tDataTypeToSparkSqlType(tStructField: TStructField): DataType = {
    val tDataType = tStructField.getDataType
    tDataType match {
      case TDataType.TINYINT => ByteType
      case TDataType.SHORT => ShortType
      case TDataType.INT => IntegerType
      case TDataType.LONG => LongType
      case TDataType.BIG_DECIMAL => DecimalType(tStructField.getPrecision, tStructField.getScale)
      case TDataType.BOOLEAN => BooleanType
      case TDataType.CSN => StringType
      case TDataType.DATE => DateType
      case TDataType.DOUBLE => DoubleType
      case TDataType.FLOAT => FloatType
      case TDataType.STRING => StringType
      case TDataType.TIMESTAMP => TimestampType
      case TDataType.VARBINARY => BinaryType
      case TDataType.CSV => StringType
      /*
    case TDataType.ARRAY => ArrayType(DataType.fromJson(""))
    case TDataType.MAP => MapType
        */
      case _ => throw new RuntimeException("暂时不支持该类型" + tDataType)
    }
  }

  /**
  // todo  入参改为 TStructField
  def tDataTypeToSparkSqlType(tDataType: TDataType): DataType = {

    tDataType match {
      case TDataType.TINYINT => ByteType
      case TDataType.SHORT => ShortType
      case TDataType.INT => IntegerType
      case TDataType.LONG => LongType
      case TDataType.BIG_DECIMAL => DecimalType.SYSTEM_DEFAULT
      case TDataType.BOOLEAN => BooleanType
      case TDataType.CSN => StringType
      case TDataType.DATE => DateType
      case TDataType.DOUBLE => DoubleType
      case TDataType.FLOAT => FloatType
      case TDataType.STRING => StringType
      case TDataType.TIMESTAMP => TimestampType
      case TDataType.VARBINARY => BinaryType
      case TDataType.CSV => StringType
      /*
    case TDataType.ARRAY => ArrayType(DataType.fromJson(""))
    case TDataType.MAP => MapType
        */
      case _ => throw new RuntimeException("暂时不支持该类型" + tDataType)
    }
  }*/

  def broadcast(model:Object, sc: SparkContext):Broadcast[_]= {
    sc.broadcast(model)
  }

  /**
    * 生成一个空的dataframe
    * @param session
    * @param structType
    * @return
    */
  def emptyDataFrame(session:SparkSession, structType: StructType):DataFrame = {
    session.createDataFrame(session.sparkContext.emptyRDD[Row], structType)
  }
}
