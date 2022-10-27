package com.eurlanda.datashire.engine.spark.rdd

import java.io._

import com.eurlanda.datashire.adapter.RemoteFileAdapter
import com.eurlanda.datashire.engine.enumeration.RemoteType
import com.eurlanda.datashire.engine.spark.NextIterator
import com.eurlanda.datashire.engine.util.{CSVExtractor, FtpUtil, UUIDUtil}
import com.eurlanda.datashire.enumeration.{EncodingType, FileType}
import com.eurlanda.datashire.utility.{XLSX2CSV, XLS2CSV}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class ExcelSharedFolderRDD[T: ClassTag]
(
        sc: SparkContext,
        ip: String,
        port: Int,
        ftpConInfo: FtpUtil.FtpConnectionInfo,
        files: Array[String],
        username: String,
        password: String,
        fileType: FileType,
        remoteType: RemoteType,
        firstDataRowNo: Int,
        serverEncoding: EncodingType,
        encoding: EncodingType,
        mapRow: (java.util.List[String]) => T
        ) extends RDD[T](sc, Nil) {

    override def getPartitions: Array[Partition] = {
        var i: Int = -1
        val partitionArray: Array[Partition] = files.map(file => {
            i += 1
            new FtpSharedFolderRDDPartition(i, ip, port, file, username, password)
        }).toArray
        printf("getPartitions partitionArray is %s.%n", partitionArray)
        partitionArray
    }


    override def compute(thePart: Partition, context: TaskContext) = new NextIterator[T] {

        val partition = thePart.asInstanceOf[FtpSharedFolderRDDPartition]
        log.info("读取 " + (if (remoteType == RemoteType.FTP) "ftp" else "共享") + "文件 === " + partition.path)
//        context.addOnCompleteCallback {
//            () => closeIfNeeded()
//        }
      context.addTaskCompletionListener{(taskContext: TaskContext) => closeIfNeeded()}
      // 读取文件，
      var iter: java.util.Iterator[java.util.List[String]] = null
      var in: InputStream = if(remoteType == RemoteType.FTP) {  // ftp 抽取
        val tmp_dir = System.getProperty("java.io.tmpdir")
        val tmp_filename = UUIDUtil.genUUID()
        downloadFileAndConvertToCSV(tmp_dir, tmp_filename, partition.path, encoding.toFtpEncoding,fileType)
        val tmpFile: File = new File(tmp_dir + File.separatorChar + tmp_filename)
        tmpFile.deleteOnExit()

        new FileInputStream(tmpFile)
      } else {  // 共享文件夹抽取
          val tmp_dir = System.getProperty("java.io.tmpdir")
          val tmp_filename = UUIDUtil.genUUID()
        downloadFileAndConvertToCSV(tmp_dir, tmp_filename, partition.path, encoding.toFtpEncoding, fileType)
          val tmpFile: File = new File(tmp_dir + File.separatorChar + tmp_filename)
          tmpFile.deleteOnExit()

          new FileInputStream(tmpFile)
          // 读取文件，
          //var iter: java.util.Iterator[java.util.List[String]] = null
        }

      // excel 文件已经被处理成CSV了
      iter = new CSVExtractor(in).iterator()

      /**
      if (!in.markSupported) {
        in = new PushbackInputStream(in, 8)
      }
      if (POIFSFileSystem.hasPOIFSHeader(in)) {
        //new HSSFWorkbook(in)
        val extractor = new ExcelExtractor(new HSSFWorkbook(in))
        extractor.setIncludeBlankCells(true)
        extractor.setIncludeCellComments(true)
        extractor.setIncludeSheetNames(false)
        iter = extractor.getRowListCellList.iterator()
      } else if (POIXMLDocument.hasOOXMLHeader(in)) {
        //new XSSFWorkbook(OPCPackage.open(in))
        val extractor = new XSSFExcelExtractor(new XSSFWorkbook(OPCPackage.open(in)))
        extractor.setIncludeBlankCells(true)
        extractor.setIncludeCellComments(false)
        extractor.setIncludeSheetNames(false)
        iter = extractor.getRowListCellList.iterator()
      } else {
        throw new IllegalArgumentException("你的excel版本目前poi解析不了")
      }
        // 1. pdf/office  list<String>
        // 2. txt文本文件 bufferedReader
      */

        // 过滤多少行  （从多少行开始）
        var lineNo = -1

        var isSkip = false
        def skipToFirstDataRow() {
            // 文档抽取跳过，采用程序变量保证
            if(!isSkip) {
                while (iter.hasNext && lineNo < firstDataRowNo) {
                    val line = iter.next()
                    logDebug("skip line " + line)
                    lineNo += 1
                }
                isSkip = true
            }
        }

        override def getNext(): T = {
            // 文档抽取跳过，采用程序变量保证
            if(!isSkip) {
                skipToFirstDataRow()
            }
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
            } catch {
                case e: Exception => logWarning("关闭文件流异常", e)
            }
        }
    }

    def downloadFile(tmp_dir: String, tmp_filename: String, path: String, strencoding: String) {
        if (remoteType == RemoteType.FTP) {
          FtpUtil.downLoadFile(ftpConInfo,path, tmp_dir + File.separator + tmp_filename);
        } else if (remoteType == RemoteType.SHARED_FILE) {
            val sharedFileAdapter: RemoteFileAdapter = new RemoteFileAdapter(ip, username, password)
            sharedFileAdapter.downSharedFile(tmp_dir, tmp_filename, path)
        }
    }

    /**
      * 下载并转化为CSV, 因为大的excel文件读取会内存溢出
      *
      * @param tmp_dir
      * @param tmp_filename
      * @param path
      * @param strencoding
      */
    def downloadFileAndConvertToCSV(tmp_dir: String, tmp_filename: String, path: String, strencoding: String, fileType: FileType) {
      // TODO 可以优化该过程,直接通过流的方式读取excel文件
      val internal_filename = "tmp_" + UUIDUtil.genUUID()
      if (remoteType == RemoteType.FTP) {
        FtpUtil.downLoadFile(ftpConInfo,path, tmp_dir + File.separator + internal_filename);
      } else if (remoteType == RemoteType.SHARED_FILE) {
        val sharedFileAdapter: RemoteFileAdapter = new RemoteFileAdapter(ip, username, password)
        sharedFileAdapter.downSharedFile(tmp_dir, internal_filename, path)
      }
      // 转换成CSV
      val internalFile: File = new File(tmp_dir + File.separatorChar + internal_filename)
      internalFile.deleteOnExit()


      // 输出文件
      val outFile = tmp_dir + File.separatorChar + tmp_filename
      val os = new FileOutputStream(outFile)
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

    def repartition(numPartitions: Int): RDD[T] = {
        printf("repartition numPartitions is %s.%n", numPartitions)
        val count = this.count()
        val parts: Int = (count / 2).toInt + 1
        //        val parts: Int = (count / 10000).toInt + 1
        val rdd = super.repartition(parts)
        printf("repartition parts is %s.%n", parts)
        printf("rdd is %s.%n", rdd)
        rdd
    }
}