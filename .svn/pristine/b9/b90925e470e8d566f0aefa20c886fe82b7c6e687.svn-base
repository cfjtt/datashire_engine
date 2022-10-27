package com.eurlanda.datashire.engine.spark.util

import java.util.{Map, List => JList}

import com.eurlanda.datashire.engine.entity.DataCell
import com.eurlanda.datashire.engine.service.SquidFlowLauncher
import com.eurlanda.datashire.engine.util.HdfsUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.commons.logging.{Log, LogFactory}

/**
  * Created by zhudebin on 16/4/28.
  */
object HdfsSquidUtil {

//  val COLUMN_DELIMITER = "COLUMN_DELIMITER"
  val ROW_DELIMITER = "ROW_DELIMITER"
  val ZIP_TYPE = "ZIP_TYPE"
  val FILE_FORMATE = "FILE_FORMATE"
  val HDFS_PATH = "HDFS_PATH"
  val SAVE_TYPE = "SAVE_TYPE"

  // partition
  // 参数 params中的key值
  val SAVE_HDFS_PATH="SAVE_HDFS_PATH" // 保存到HDFS的路径
  val SAVE_MODEL_INDEX = "SAVE_MODEL_INDEX" // Int 保存模式
  val COMPRESSION_TYPE_INDEX="COMPRESSION_TYPE_INDEX" // Int  压缩方式
  val COLUMN_DELIMITER="COLUMN_DELIMITER"  // String 列分隔符
  val HOST="HOST"  //  ip:port
  val FALL_FILE_FORMAT_INDEX="FALL_FILE_FORMAT_INDEX" // 落地文件类型id
  val HDFS_SPACE_LIMIT = "HDFS_SPACE"
  private val log: Log = LogFactory.getLog(HdfsSquidUtil.getClass)

  /**
    *
    * @param sc
    * @param previousRDD
    * @param fallColumnNames 落地列名
    * @param fallColumnIds  落地ids
    * @param partitionColumnIds 分区列ids
    * @param params
    */
  def saveToHdfsByPartitions(sc:SparkContext, previousRDD: JavaRDD[java.util.Map[Integer, DataCell]], fallColumnNames: Array[String] ,
                 fallColumnIds: Array[Integer], partitionColumnIds: Array[Integer], params : java.util.Map[String,Object]
                ): Unit = {

    val saveHDFSPath = params.get(SAVE_HDFS_PATH).toString
    val saveModelIndex = params.get(SAVE_MODEL_INDEX).toString.toInt
    val compressionTypeIndex = params.get(COMPRESSION_TYPE_INDEX).toString.toInt
    var columnDelimiter:String = null  // 只有落地成text格式才会用到
    val host = params.get(HOST).toString
    val fallFileFormatIndex = params.get(FALL_FILE_FORMAT_INDEX).toString.toInt
    val hdfsSpaceLimit = params.get(HDFS_SPACE_LIMIT).toString.toInt
    val savePath = "hdfs://" + host + saveHDFSPath

    try {
      val saveModel = getSaveModel(saveModelIndex)
      if (isDirectoryOrFileExists(savePath) && saveModel == SaveMode.ErrorIfExists) {
        throw new IllegalArgumentException(host + "上已经存在同名目录或文件" + saveHDFSPath)
      } else if (isDirectoryOrFileExists(savePath) && saveModel == SaveMode.Ignore) {
        return
      } else if (!isDirectoryOrFileExists(savePath) && saveModel != SaveMode.ErrorIfExists) {
        HdfsUtil.mkdir(savePath)
        HdfsUtil.setAllACL(savePath)
      }
      /**
        * 判断文件是否超过规定大小
        */
      if(!isDirectoryOverLimitSpaces(savePath,hdfsSpaceLimit)){
        throw new DSQuotaExceededException
      }
      val partitionColumnNames = getPartitionColumnNames(fallColumnIds,fallColumnNames,partitionColumnIds)
      val fallFileFormat = getFallFileFormat(fallFileFormatIndex)
      if(fallFileFormat.equalsIgnoreCase("text")){
        columnDelimiter = if(params.get(COLUMN_DELIMITER)==null) null else params.get(COLUMN_DELIMITER).toString
        if(columnDelimiter == null){
             throw new IllegalArgumentException("参数columnDelimiter是null")
        }
      }
      val compressionType = getCompressionType(fallFileFormat,compressionTypeIndex)

      if (partitionColumnIds != null) {
        if (partitionColumnIds.length >= fallColumnIds.length) {
          throw new IllegalArgumentException("分区列的数量" + partitionColumnIds.length + "应小于落地列的数量" + fallColumnIds.length)
        }
      }
      if (fallColumnNames == null || fallColumnNames.length == 0) {
        throw new IllegalArgumentException("没有落地列名")
      }

      var dataFrame = getDataFrame(previousRDD,fallColumnIds,fallColumnNames)
      //添加hdfs限额的参数
      dataFrame.sparkSession.conf.set(HDFS_SPACE_LIMIT,hdfsSpaceLimit)
      if (fallFileFormat.equalsIgnoreCase("text")) {
        dataFrame = concatColumns(dataFrame,fallColumnNames,partitionColumnNames,columnDelimiter) // text 时需要把所有非分区的列合并成一列，用列分隔符分开
      }
      if (partitionColumnNames == null || partitionColumnNames.length == 0) {
        //没有分区列
        if (compressionType == null) {
          dataFrame.write.format(fallFileFormat).mode(saveModel).save(savePath)
        } else {
          dataFrame.write.option("compression", compressionType).format(fallFileFormat).mode(saveModel).save(savePath)
        }
      } else {
        if (compressionType == null) {
          dataFrame.write.partitionBy(partitionColumnNames: _*).format(fallFileFormat).mode(saveModel).save(savePath)
        } else {
          dataFrame.write.partitionBy(partitionColumnNames: _*).format(fallFileFormat).option("compression", compressionType).mode(saveModel).save(savePath)
        }
      }
    } catch {
      case e: org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException=>{
        var msg = e.getMessage()
        log.error(msg)
        e.printStackTrace()
        if(msg.contains("The maximum path component name limit of")){
          msg =msg.replace("The maximum path component name limit of ","路径大小").replace("is exceeded: limit=","不能超过").replace(" length=","实际大小")+"字节"
        }
        throw new java.lang.RuntimeException(msg)
      }
      case e: org.apache.spark.sql.AnalysisException => {
        var msg = e.getMessage().replace("\n","").replace("  ","")
        log.error(msg)
        e.printStackTrace()
        if (msg.contains("Requested partitioning does not match existing partitioning")) {
          msg = msg.replace("Requested partitioning does not match existing partitioning","与已有分区不匹配")
          msg = msg.replace("Existing partitioning columns","已有分区").replace("Requested partitioning columns","，新分区")
          throw new java.lang.RuntimeException(msg)
        } else {
          throw e
        }
      }
      case e: org.apache.hadoop.hdfs.protocol.DSQuotaExceededException =>{
        log.error(e.getMessage)
        e.printStackTrace()
        throw new java.lang.RuntimeException("文件大小超过hdfs限额")
      }
      case e : Throwable => {
        val msg = e.getMessage
        log.error(msg)
        if (msg.contains("grows beyond 64 KB")) {
          throw new java.lang.RuntimeException("调用方法超过 64 KB")
        } else if(msg.contains("The DiskSpace quota") || msg.contains("is exceeded") || e.getCause.getCause.isInstanceOf[DSQuotaExceededException]) {
          throw new java.lang.RuntimeException("文件大小超过hdfs限额")
        }
        throw e
      }
    }
  }

  /**
    *
    * @param path
    * @return
    */
  def isDirectoryExists(path: String): Boolean = {
    val srcPath = new Path(path)
    val fs = srcPath.getFileSystem(new Configuration)
    val isExists = fs.isDirectory(srcPath)
    fs.close()
    isExists
  }

  /**
    * 判断文件是否超过限额
    * @param path
    * @return
    */
  def isDirectoryOverLimitSpaces(path:String,limit:Int) : Boolean={
    val srcPath = new Path(path)
    val fs = srcPath.getFileSystem(new Configuration())
    if(limit>0) {
      if (fs.getContentSummary(srcPath).getLength() / 1024 / 1024 > limit) {
        false
      } else {
        true
      }
    } else {
      true
    }
  }

  /**
    *
    * @param path
    * @return
    */
  def isDirectoryOrFileExists(path: String): Boolean = {
    val srcPath = new Path(path)
    val fs = srcPath.getFileSystem(new Configuration)
    val isExists = fs.exists(srcPath)
    fs.close()
    isExists
  }

  /**
    * 把RDD转换为 DataFrame
    *
    * @param javaRDD
    * @return
    */
  private def getDataFrame(javaRDD: JavaRDD[Map[Integer, DataCell]],fallColumnIds:Array[Integer],fallColumnNames:Array[String]): DataFrame = {
    val sparkSession = SquidFlowLauncher.getSparkSession
    val sparkContext = sparkSession.sparkContext
    import org.apache.spark.sql.Row
    val fallColumnIdsBroad = sparkContext.broadcast(fallColumnIds)
    val rowRdd = javaRDD.rdd.mapPartitions(partition => {
      val fallColumnIdsBroadValue = fallColumnIdsBroad.value
      partition.map(kvMap => {
        val kvValueArr = for (columnId <- fallColumnIdsBroadValue) yield {
          // value 变成 Array
          if (kvMap.get(columnId) == null || kvMap.get(columnId).getData == null) {
            null
          } else {
            kvMap.get(columnId).getData.toString
          }
        }
        Row.fromSeq(kvValueArr)
      })
    })
    val structType = StructType(fallColumnNames.map(fieldName => StructField(fieldName, StringType, true)))
    val structTypeBroadcast = sparkContext.broadcast(structType)
    sparkSession.createDataFrame(rowRdd, structTypeBroadcast.value)
  }

  /**
    * 保存为text格式时，需要把所有非分区的列合并成一列，用列分隔符分开。如果没有分区列时，则把所有列都合并
    *
    * @param dataFrame
    * @return
    */
  private def concatColumns(dataFrame: DataFrame,fallColumnNames:Array[String] ,partitionColumnNames:Array[String],columnDelimiter:String): DataFrame = {
    if (columnDelimiter == null) {
      throw new NullPointerException("列分隔符是null")
    }
    val notPartitionColumnNames =
      if (partitionColumnNames == null || partitionColumnNames.length == 0) {
        // 没有分区列时，把所有列都合并
        fallColumnNames
      } else {
        //有分区列时只合并非分区列
        fallColumnNames.diff(partitionColumnNames)
      }
    // var concatColumnsName = notPartitionColumnNames.mkString("_")
    var concatColumnsName = "concatColumnsName"
    while (fallColumnNames.contains(concatColumnsName)) {
      // 防止合并后的列重名
      concatColumnsName = concatColumnsName + "_1"
    }
    import org.apache.spark.sql.functions.{concat, lit}
    var concatDf = dataFrame.withColumnRenamed(notPartitionColumnNames.head, concatColumnsName)
    concatDf = concatDf.na.fill("\\N", Seq(concatColumnsName))
    for (colName <- notPartitionColumnNames.tail) {
      concatDf = concatDf.na.fill("\\N", Seq(colName))
      concatDf = concatDf.withColumn(concatColumnsName, concat(concatDf.col(concatColumnsName), lit(columnDelimiter), concatDf.col(colName)))
      concatDf = concatDf.drop(colName)
    }
    concatDf
  }

  /**
    * 保存模式
    *
    * @return
    */
  private def getSaveModel(saveModelIndex:Int): SaveMode = {
    saveModelIndex match {
      case 1 => SaveMode.Append
      case 2 => SaveMode.Overwrite
      case 3 => SaveMode.ErrorIfExists
      case 4 => SaveMode.Ignore
      case _ => throw new IllegalArgumentException("不能按模式" + saveModelIndex + "保存")
    }
  }

  /**
    * 压缩方式
    *
    * @return
    */
  private def getCompressionType(fallFileFormat:String,compressionTypeIndex:Int): String = {
    val fileFormat = fallFileFormat
    val compressionTypes = Array("none", "bzip2", "gzip", "lz4", "snappy", "deflate", "lzo", "zlib")
    if (fileFormat.equalsIgnoreCase("text") || fileFormat.equalsIgnoreCase("csv") || fileFormat.equalsIgnoreCase("json")) {
      compressionTypeIndex match {
        case 0 => compressionTypes.apply(0) //"none"
        case 1 => compressionTypes.apply(1) // "bzip2"
        case 2 => compressionTypes.apply(2) // "gzip"
        case 3 => compressionTypes.apply(3) // "lz4"
        case 4 => compressionTypes.apply(4) // "snappy"
        case 5 => compressionTypes.apply(5) // "deflate"
        case _ => throw new IllegalArgumentException("不能按格式" + compressionTypeIndex + "压缩")
      }
    } else if (fileFormat.equalsIgnoreCase("orc")) {
      compressionTypeIndex match {
        case 0 => compressionTypes.apply(0) //"none"
        case 4 => compressionTypes.apply(4) //"snappy"
        case 6 => compressionTypes.apply(6) //"lzo"
        case 7 => compressionTypes.apply(7) //"zlib"
        case _ => throw new IllegalArgumentException("不能按格式" + compressionTypeIndex + "压缩")
      }
    } else if (fileFormat.equalsIgnoreCase("parquet")) {
      compressionTypeIndex match {
        case 0 => compressionTypes.apply(0) //"none"
        case 2 => compressionTypes.apply(2) //"gzip"
        case 4 => compressionTypes.apply(4) //"snappy"
        case 6 => compressionTypes.apply(6) //"lzo"
        case _ => throw new IllegalArgumentException("不能按格式" + compressionTypeIndex + "压缩")
      }
    } else {
      throw new IllegalArgumentException("不能按格式" + compressionTypeIndex + "压缩")
    }
  }

  /**
    * 落地到HDFS上的文件格式
    *
    * @return
    */
  private def getFallFileFormat(fallFileFormatIndex :Int): String = {
    fallFileFormatIndex match {
      case 1 => "text"
      case 3 => "parquet"
      case 5 => "csv"
      case 6 => "json"
      case 7 => "orc"
      case _ => throw new IllegalArgumentException("不能按格式" + fallFileFormatIndex + "落地")
    }
  }

  /**
    * 分区列名
    *
    * @return
    */
  private def getPartitionColumnNames(fallColumnIds: Array[Integer],fallColumnNames:Array[String],partitionColumnIds: Array[Integer]): Array[String] = {
    if (partitionColumnIds == null) {
      return null
    }
    val fallIdName = fallColumnIds.zip(fallColumnNames)
    fallIdName.filter(x => partitionColumnIds.contains(x._1)).map(x => x._2)
  }
}
