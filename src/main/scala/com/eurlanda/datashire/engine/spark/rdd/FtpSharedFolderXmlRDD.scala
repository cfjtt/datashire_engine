package com.eurlanda.datashire.engine.spark.rdd

import java.io.{File, FileReader}
import javax.xml.stream.events.XMLEvent
import javax.xml.stream.{XMLEventReader, XMLInputFactory}

import com.eurlanda.datashire.adapter.RemoteFileAdapter
import com.eurlanda.datashire.engine.enumeration.RemoteType
import com.eurlanda.datashire.engine.spark.NextIterator
import com.eurlanda.datashire.engine.util.{FtpUtil, UUIDUtil}
import com.eurlanda.datashire.enumeration.{EncodingType, FileType}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
 * Created by zhudebin on 14-1-9.
 * FTP文件RDD， 每一个文件分成一片，
 *
 */
//TODO 分片规则优化 需要使用hadoopRDD处理
class FtpSharedFolderXmlRDD[T: ClassTag]
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
        encoding: EncodingType,
        mapRow: (XMLEvent, XMLEventReader) => T
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
        log.info("  读取ftp 文件 === " + partition.path)
//        context.addOnCompleteCallback {
//            () => closeIfNeeded()
//        }
        context.addTaskCompletionListener{(taskContext: TaskContext) => closeIfNeeded()}
        val tmp_dir = System.getProperty("java.io.tmpdir")
        val tmp_filename = UUIDUtil.genUUID()

        downloadFile(tmp_dir, tmp_filename, partition.path,
//          protocol,
          encoding)
        val tmpFile: File = new File(tmp_dir + File.separatorChar + tmp_filename)
        tmpFile.deleteOnExit()
        // 读取文件，
        val xif: XMLInputFactory = XMLInputFactory.newInstance
        val reader: XMLEventReader = xif.createXMLEventReader(new FileReader(tmpFile))

        override def getNext(): T = {
            if (reader.hasNext) {
              val xmlEvent: XMLEvent = reader.nextEvent
              try {
                mapRow(xmlEvent, reader)
              } catch {
                case e: Exception => {
                  log.error(e.getMessage)
                  null.asInstanceOf[T]
                }
              }
            } else {
                finished = true
                null.asInstanceOf[T]
            }
        }

        //Todo 待优化 大文件 不能分布式处理
        def downloadFile(tmp_dir: String, tmp_filename: String, path: String,
//                         protocol: Int,
                         encoding: EncodingType) {
            if (remoteType == RemoteType.FTP) {
              FtpUtil.downLoadFile(ftpConInfo,path, tmp_dir + File.separator + tmp_filename);
            } else if (remoteType == RemoteType.SHARED_FILE) {
                val sharedFileAdapter: RemoteFileAdapter = new RemoteFileAdapter(ip, username, password)
                sharedFileAdapter.downSharedFile(tmp_dir, tmp_filename, path)
            }
        }

        override def close() {
            try {
                reader.close()
            } catch {
                case e: Exception => logWarning("关闭文件流异常", e)
            }
        }
    }

}