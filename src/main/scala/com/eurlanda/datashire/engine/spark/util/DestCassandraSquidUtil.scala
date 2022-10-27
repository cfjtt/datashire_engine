package com.eurlanda.datashire.engine.spark.util

import com.eurlanda.datashire.engine.entity.TStructField
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/**
  * Created by zhudebin on 2017/5/3.
  */
object DestCassandraSquidUtil {

  /**
    * 保存数据到cassandra中
    * @param dataFrame
    * @param destColumns
    * @param tableName
    * @param cassandraConnectionInfo "keyspace","cluster",
    *                                "cassandra.validate_type",
    *                                "engine.spark.cassandra.username",
    *                                "engine.spark.cassandra.password",
    *                                "engine.spark.cassandra.host",
    *                                "engine.spark.cassandra.port"
    * @param saveMode 只支持append,overwrite两种
    */
  def save(dataFrame: DataFrame,
           destColumns: java.util.List[(String, TStructField)],
           cassandraConnectionInfo: java.util.HashMap[String, String],
           tableName: String, saveMode: SaveMode): Unit = {
    var outSchema = new StructType()
    import scala.collection.JavaConversions._
    for(t2 <- destColumns) {
      outSchema = outSchema.add(t2._1, EngineUtil.tDataTypeToSparkSqlType(t2._2), t2._2.isNullable)
    }

    val inSchema = dataFrame.schema

    val options = if(cassandraConnectionInfo.get("cassandra.validate_type").toString.toInt == 1) {
      Map( "table" -> tableName,
        "keyspace" -> cassandraConnectionInfo.get("keyspace"),
        "cluster" -> cassandraConnectionInfo.get("cluster"),
        "cassandra.validate_type" -> cassandraConnectionInfo.get("cassandra.validate_type"),
        "engine.spark.cassandra.username" -> cassandraConnectionInfo.get("engine.spark.cassandra.username"),
        "engine.spark.cassandra.password" -> cassandraConnectionInfo.get("engine.spark.cassandra.password"),
        "engine.spark.cassandra.host" -> cassandraConnectionInfo.get("engine.spark.cassandra.host"),
        "engine.spark.cassandra.port" -> cassandraConnectionInfo.get("engine.spark.cassandra.port"))
    } else {
      Map( "table" -> tableName,
        "keyspace" -> cassandraConnectionInfo.get("keyspace"),
        "cluster" -> cassandraConnectionInfo.get("cluster"),
        "cassandra.validate_type" -> cassandraConnectionInfo.get("cassandra.validate_type"),
        "engine.spark.cassandra.username" -> "__no__",
        "engine.spark.cassandra.password" -> "__no__",
        "engine.spark.cassandra.host" -> cassandraConnectionInfo.get("engine.spark.cassandra.host"),
        "engine.spark.cassandra.port" -> cassandraConnectionInfo.get("engine.spark.cassandra.port"))
    }

    dataFrame.map(row => {
      val data = for(t2 <- destColumns)  yield{
        val index = inSchema.fieldIndex(t2._2.getName)
        row(index)
      }
      Row.fromSeq(data.toSeq)
    })(RowEncoder(outSchema)).write.format("org.apache.spark.sql.cassandra")
      .mode(saveMode)
      .options(options)
      .save()
  }

}
