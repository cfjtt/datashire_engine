package com.eurlanda.datashire.engine.spark.rdd

import java.io.{BufferedReader, StringReader}

import com.eurlanda.datashire.engine.enumeration.RowDelimiterPosition
import com.eurlanda.datashire.engine.spark.NextIterator
import com.eurlanda.datashire.enumeration.{EncodingType, FileType}
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.pdfbox.pdfparser.PDFParser
import org.apache.pdfbox.util.PDFTextStripper
import org.apache.poi.POITextExtractor
import org.apache.poi.extractor.ExtractorFactory
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
 * hdfs rdd
 * Created by Juntao.Zhang on 2014/6/16.
 */
class HdfsWordPdfRDD[T: ClassTag]
(sc: SparkContext,
 hdfsUrl: String,
 paths: Array[String],
 fileType: FileType,
 firstDataRowNo: Int,
 encoding: EncodingType,
 mapRow: (String) => T,
 position: RowDelimiterPosition,
 rowDelimiter: String) extends DocExtractRDD[T](sc, firstDataRowNo, position, rowDelimiter) {

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
        val pt = new Path(partition.path)
        val conf = new Configuration()
        conf.set("fs.defaultFS", hdfsUrl)
        val fs = FileSystem.get(conf)
        val in = fs.open(pt)
        var br: BufferedReader = null

        if (FileType.isWord(fileType)) {
            val extractor: POITextExtractor = ExtractorFactory.createExtractor(in)
            br = new BufferedReader(new StringReader(extractor.getText))
        } else if (fileType == FileType.PDF) {
            val parser: PDFParser = new PDFParser(in)
            parser.parse()
            br = new BufferedReader(new StringReader(new PDFTextStripper().getText(parser.getPDDocument)))
        } else {
            throw new RuntimeException("没有匹配的文件类型")
        }

        skipToFirstDataRow(br)

        override def getNext(): T = {
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
                br.close()
                fs.close()
            } catch {
                case e: Exception => logWarning("关闭文件流异常", e)
            }
        }
    }

}
