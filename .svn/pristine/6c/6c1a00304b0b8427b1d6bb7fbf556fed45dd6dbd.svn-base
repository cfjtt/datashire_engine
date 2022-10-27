package com.eurlanda.datashire.engine.spark.util

import com.eurlanda.datashire.engine.entity.{DataCell, TColumn, TNoSQLDataSource}
import org.apache.spark.CustomJavaSparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by zhudebin on 16/8/8.
  */
object MongoSquidUtil {

  def saveToMongo(dataSource : TNoSQLDataSource,
                  columns: java.util.Set[TColumn],
                  rdd: RDD[java.util.Map[Integer, DataCell]],
                  cjsc: CustomJavaSparkContext, truncateExistingData: Boolean) {
    cjsc.fallDataToMongo(dataSource, columns, rdd.toJavaRDD(), truncateExistingData)
  }

}
