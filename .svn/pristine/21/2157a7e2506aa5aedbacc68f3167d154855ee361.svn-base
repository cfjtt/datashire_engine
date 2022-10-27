package com.eurlanda.datashire.engine.spark.rdd

import java.io._

import com.eurlanda.datashire.adapter.RemoteFileAdapter
import com.eurlanda.datashire.engine.enumeration.{RemoteType, RowDelimiterPosition}
import com.eurlanda.datashire.engine.spark.NextIterator
import com.eurlanda.datashire.engine.util.{FtpUtil, UUIDUtil}
import com.eurlanda.datashire.enumeration.{EncodingType, FileType}
import org.apache.commons.io.ByteOrderMark
import org.apache.commons.io.input.BOMInputStream
import org.apache.pdfbox.pdfparser.PDFParser
import org.apache.pdfbox.util.PDFTextStripper
import org.apache.poi.POITextExtractor
import org.apache.poi.extractor.ExtractorFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
 * txt word pdf Ftp 共享文件夹 RDD
 * Created by Juntao.Zhang on 2014/6/28.
 */
class FtpSharedFolderTxtWordPdfRDD[T: ClassTag]
(sc: SparkContext,
 ip: String,
 port: Int,
 ftpConInfo: FtpUtil.FtpConnectionInfo,
 files: Array[String],
 username: String,
 password: String,
 fileType: FileType,
 remoteType: RemoteType,
 firstDataRowNo: Int,
 serverEncoding: EncodingType,  // ftp 服务器编码
 encoding: EncodingType,  // 文件编码
 mapRow: (String) => T,
 position: RowDelimiterPosition,
 rowDelimiter: String) extends DocExtractRDD[T](sc, firstDataRowNo, position, rowDelimiter) {

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
        val tmp_dir = System.getProperty("java.io.tmpdir")
        val tmp_filename = UUIDUtil.genUUID()
        downloadFile(tmp_dir, tmp_filename, partition.path)
        val tmpFile: File = new File(tmp_dir + File.separatorChar + tmp_filename)
        tmpFile.deleteOnExit()
        // 读取文件，
        var br: BufferedReader = null
        if (fileType == FileType.TXT) {
          // 添加对BOM的处理
          /**
          * <li>UTF-8 - {@link ByteOrderMark#UTF_8}</li>
          * <li>UTF-16BE - {@link ByteOrderMark#UTF_16LE}</li>
          * <li>UTF-16LE - {@link ByteOrderMark#UTF_16BE}</li>
          * <li>UTF-32BE - {@link ByteOrderMark#UTF_32LE}</li>
          * <li>UTF-32LE - {@link ByteOrderMark#UTF_32BE}</li>
          */
            br = new BufferedReader(new InputStreamReader(
              new BOMInputStream(new FileInputStream(tmpFile),
                ByteOrderMark.UTF_8, ByteOrderMark.UTF_16BE,
                ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_32BE,
                ByteOrderMark.UTF_32LE), encoding.toFtpEncoding))
        } else if (FileType.isWord(fileType)) {
            val extractor: POITextExtractor = ExtractorFactory.createExtractor(new FileInputStream(tmpFile))
            br = new BufferedReader(new StringReader(extractor.getText))
        } else if (fileType == FileType.PDF) {
            val parser: PDFParser = new PDFParser(new FileInputStream(tmpFile))
            parser.parse()
            br = new BufferedReader(new StringReader(new PDFTextStripper().getText(parser.getPDDocument)))
        } else {
            throw new RuntimeException("没有匹配的文件类型")
        }

        var isSkip = false
        if(!isSkip) {
            skipToFirstDataRow(br)
            isSkip = true
        }

        override def getNext(): T = {
            // 文档抽取跳过，采用程序变量保证
            if(!isSkip) {
                skipToFirstDataRow(br)
                isSkip = true
            }
            val line = readRecord(br)
            if (line != null) {
                mapRow(line)
            } else {
                finished = true
                null.asInstanceOf[T]
            }
        }

        override def close() {
            try {

            } catch {
                case e: Exception => logWarning("关闭文件流异常", e)
            }
        }
    }

    def downloadFile(tmp_dir: String, tmp_filename: String, path: String) {
        if (remoteType == RemoteType.FTP) {
          FtpUtil.downLoadFile(ftpConInfo,path, tmp_dir + File.separator + tmp_filename);
        } else if (remoteType == RemoteType.SHARED_FILE) {
            val sharedFileAdapter: RemoteFileAdapter = new RemoteFileAdapter(ip, username, password)
            sharedFileAdapter.downSharedFile(tmp_dir, tmp_filename, path)
        }
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
