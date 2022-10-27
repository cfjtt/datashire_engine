package com.eurlanda.datashire.engine.entity

import com.eurlanda.datashire.engine.spark.util.HdfsSquidUtil
import org.apache.commons.logging.LogFactory
import org.apache.spark.api.java.JavaSparkContext

/**
  * hdfs 分区落地
  * 落地列包含分区列
  *
  * 以下数据类型不能作为分区列
  * TDataType.ARRAY, TDataType.BINARYSTREAM, TDataType.CSN,
      *TDataType.CSV, TDataType.LABELED_POINT, TDataType.MAP, TDataType.RATING, TDataType.TCOLUMN,
      *TDataType.TIME, TDataType.TIMESTAMP, TDataType.VARBINARY
  */
class THdfsPartitionFallSquid extends TSquid {

  private val log = LogFactory.getLog(classOf[THdfsPartitionFallSquid])
  setType(TSquidType.DEST_HDFS_SQUID)

  var previousSquid: TSquid = null
  var fallColumnNames: Array[String] = null // 落地列名，包含分区列
  var fallColumnIds: Array[Integer] = null  // 落地列Id
  var partitionColumnIds: Array[Integer] = null // 分区列Id
  var params : java.util.Map[String,Object] = null // 参数,包括以下参数
  /* SAVE_HDFS_PATH  ： 保存到HDFS的路径
      SAVE_MODEL_INDEX ： Int 保存模式
      COMPRESSION_TYPE_INDEX ：Int  压缩方式
      COLUMN_DELIMITER ：String 列分隔符
      HOST  是 ip:port
      FALL_FILE_FORMAT_INDEX 落地文件类型id */

  override def run(jsc: JavaSparkContext): Object = {
    log.info("HDFS分区落地")
    if (previousSquid.outRDD == null) {
      previousSquid.runSquid(jsc)
    }
    //添加落地hdfs的限额
    params.put(HdfsSquidUtil.HDFS_SPACE_LIMIT,getCurrentFlow.getHdfsSpaceLimit+"")
    HdfsSquidUtil.saveToHdfsByPartitions(jsc.sc, previousSquid.outRDD, fallColumnNames, fallColumnIds, partitionColumnIds, params)
    return null
  }
}

