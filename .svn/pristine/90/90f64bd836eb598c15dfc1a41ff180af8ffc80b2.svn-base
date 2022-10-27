package com.eurlanda.datashire.engine.spark.rdd

import java.io._

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
 * csv excel
 * FTP文件RDD， 每一个文件分成一片，
 */
//TODO 分片规则优化 对TXT文本类型的能对大文件进行分批处理
class LogFtpSharedFolderRDD[T: ClassTag]
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
        mapRow: (String) => T
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
        val tmp_dir = System.getProperty("java.io.tmpdir")
        val tmp_filename = UUIDUtil.genUUID()
        downloadFile(tmp_dir, tmp_filename, partition.path, (if(serverEncoding!=null){serverEncoding.toFtpEncoding} else null))
        val tmpFile: File = new File(tmp_dir + File.separatorChar + tmp_filename)
        tmpFile.deleteOnExit()
        // todo 路径 下载文件， 保存到HDFS
        // 读取文件，
        var br: BufferedReader = null
        if (fileType == FileType.LOG||fileType == FileType.CSV) {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(tmpFile), encoding.toFtpEncoding))
        } else {
            throw new RuntimeException("没有匹配的文件类型")
        }

        // 1. pdf/office  list<String>
        // 2. txt文本文件 bufferedReader

        // 过滤多少行  （从多少行开始）
        var lineNo = -1

        var isSkip = false
        def skipToFirstDataRow() {
            // 文档抽取跳过，采用程序变量保证
            if(!isSkip) {
                var _line:String = ""
                while (_line != null && lineNo < firstDataRowNo) {
                    _line = br.readLine()
                    logDebug("skip line " + _line)
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
            val line: String = br.readLine()

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

    def downloadFile(tmp_dir: String, tmp_filename: String, path: String, strencoding: String) {
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