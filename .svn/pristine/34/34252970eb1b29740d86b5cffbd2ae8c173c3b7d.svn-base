package com.eurlanda.datashire.engine.spark.rdd

import java.io.{File, FileInputStream, FileOutputStream}

import com.eurlanda.datashire.engine.spark.NextIterator
import com.eurlanda.datashire.engine.util.{FtpUtil, CSVExtractor, UUIDUtil}
import com.eurlanda.datashire.enumeration.{EncodingType, FileType}
import com.eurlanda.datashire.utility.{XLS2CSV, XLSX2CSV}
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
 * hdfs rdd
 * Todo Juntao.zhang 需要使用前台的ip port
 * Created by Juntao.Zhang on 2014/6/16.
 */
class HdfsExcelRDD[T: ClassTag]
(
        sc: SparkContext,
        hdfsUrl: String,
        paths: Array[String],
        fileType: FileType,
        firstDataRowNo: Int,
        encoding: EncodingType,
        mapRow: (java.util.List[String]) => T
        ) extends RDD[T](sc, Nil) {

    override def getPartitions: Array[Partition] = {
        var i: Int = -1
        val partitionArray: Array[Partition] = paths.map(path => {
            i += 1
            new HdfsRDDPartition(i, path)
        }).toArray
        partitionArray
    }

    override def compute(thePart: Partition, context: TaskContext) = new NextIterator[T] {
        val partition = thePart.asInstanceOf[HdfsRDDPartition]
        log.info("读取hdfs 文件 === " + partition.path)
//        context.addOnCompleteCallback {
//            () => closeIfNeeded()
//        }
        context.addTaskCompletionListener{(taskContext: TaskContext) => closeIfNeeded()}
        // 下载并转换成CSV
        val tmp_filename = "tmp_" + UUIDUtil.genUUID()
        val tmp_dir = System.getProperty("java.io.tmpdir")
        downloadFileAndConvertToCSV(tmp_dir, tmp_filename, partition.path, hdfsUrl)

        val tmpFile = new File(tmp_dir + File.separatorChar + tmp_filename)   // 转成CSV的文件
        tmpFile.deleteOnExit()

        logDebug("转换CSV完成,path:" + tmpFile.getAbsolutePath)
        // 转换完成----
        val in = new FileInputStream(tmpFile)   // csv的输入流
        var iter: java.util.Iterator[java.util.List[String]] = new CSVExtractor(in).iterator()

        /**

          * if (FileType.isExcel(fileType)) {
          * if (FileType.XLS == fileType) {
          * val extractor = new ExcelExtractor(new POIFSFileSystem(in))
          * extractor.setIncludeBlankCells(true)
          * extractor.setIncludeCellComments(true)
          * extractor.setIncludeSheetNames(false)
          * iter = extractor.getRowListCellList.iterator()
          * } else {
          * val extractor = new XSSFExcelExtractor(new XSSFWorkbook(in))
          * extractor.setIncludeBlankCells(true)
          * extractor.setIncludeCellComments(false)
          * extractor.setIncludeSheetNames(false)
          * iter = extractor.getRowListCellList.iterator()
          * }
          * } else {
          * throw new RuntimeException("没有匹配的文件类型")
          * }
        */

        var lineNo = -1
        while (iter.hasNext && lineNo < firstDataRowNo) {
            println("skip line " + iter.next())
            lineNo += 1
        }

        override def getNext(): T = {
            if (iter.hasNext) {
                mapRow(iter.next())
            } else {
                finished = true
                null.asInstanceOf[T]
            }
        }

        override def close() {
            try {
                in.close()
                tmpFile.delete()
            } catch {
                case e: Exception => logWarning("关闭文件流异常", e)
            }
        }
    }

    /**
      * 下载并转化为CSV, 因为大的excel文件读取会内存溢出
      *
      * @param tmp_dir
      * @param tmp_filename
      * @param path
      */
    def downloadFileAndConvertToCSV(tmp_dir: String, tmp_filename: String, path: String, hdfsUrl: String) {
      // 下载文件
      val pt = new Path(path)
      val conf = new Configuration()
      conf.set("fs.defaultFS", hdfsUrl)
      val fs:FileSystem = FileSystem.get(conf)

      // 转换文件----
      val internal_filename = "tmp_" + UUIDUtil.genUUID()
      val tmp_dir = System.getProperty("java.io.tmpdir")
      val internalFile:File = new File(tmp_dir + File.separatorChar + internal_filename)   // 转成CSV的文件
      // 复制到本地,中间临时文件
      FileUtil.copy(fs, pt, internalFile, false, conf)

      // 转换成CSV
//      val internalFile: File = new File(tmp_dir + File.separatorChar + internal_filename)
//      internalFile.deleteOnExit()


      // 输出文件
      val outFile = tmp_dir + File.separatorChar + tmp_filename
      val os = new FileOutputStream(outFile)
      log.info("转换hdfs excel文件 === 目录:" + outFile)
      if(fileType == FileType.XLS) {
        val x2c:XLS2CSV = new XLS2CSV(internalFile, os)
        x2c.process()
      } else if(fileType == FileType.XLSX) {
        val x2c:XLSX2CSV = new XLSX2CSV(internalFile, os);
        x2c.process()
      } else {
        new RuntimeException("错误的excel类型:" + fileType)
      }
      internalFile.delete();

    }
}
