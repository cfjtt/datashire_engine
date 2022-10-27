package com.eurlanda.datashire.engine.spark.util

import com.eurlanda.datashire.engine.entity.TStructField
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Utils, DataFrame, Row, SaveMode}

/**
  * Created by zhudebin on 2017/5/3.
  */
object DestSystemHiveSquidUtil {

  def save(dataFrame: DataFrame,
           hiveColumns: java.util.List[(String, TStructField)],
           tableName: String, saveMode: SaveMode): Unit = {
    Utils.getSessionState(dataFrame.sparkSession).conf.setConfString("hive.exec.dynamic.partition.mode", "nonstrict")
    var outSchema = new StructType()
    import scala.collection.JavaConversions._
    for(t2 <- hiveColumns) {
      outSchema = outSchema.add(t2._1, EngineUtil.tDataTypeToSparkSqlType(t2._2), t2._2.isNullable)
    }

    val inSchema = dataFrame.schema
    dataFrame.map(row => {
      val data = for(t2 <- hiveColumns)  yield{
        val index = inSchema.fieldIndex(t2._2.getName)
        row(index)
      }
      Row.fromSeq(data.toSeq)
    })(RowEncoder(outSchema)).write.mode(saveMode)
      .insertInto(tableName)
//      .saveAsTable(tableName) 只适用于sparkSQL创建的表
  }

}
