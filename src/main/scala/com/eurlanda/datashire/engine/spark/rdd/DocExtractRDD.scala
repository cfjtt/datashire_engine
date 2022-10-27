package com.eurlanda.datashire.engine.spark.rdd

import java.io.BufferedReader
import java.util.regex.{Matcher, Pattern}

import com.eurlanda.datashire.engine.enumeration.RowDelimiterPosition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * doc extract squid 抽象类
 * 获得doc多行一个record 或者 一行多个record rdd
 * Created by Juntao.Zhang on 2014/6/28.
 */
abstract class DocExtractRDD[T: ClassTag]
(sc: SparkContext,
 firstDataRowNo: Int,
 position: RowDelimiterPosition,
 rowDelimiter: String) extends RDD[T](sc, Nil) {

    val _list: java.util.LinkedList[String] = new java.util.LinkedList[String]
    val _bufferSize = 3000
    // 一次取多少个buffer的内容装载到List中，控制内存使用
    val _recordBufferNum = 20
    var _count: Int = 0
    var _record: String = ""

    //跳到数据行
    def skipToFirstDataRow(br: BufferedReader) {
        var lineNo = -1
        // 过滤多少行  （从多少行开始）
        var line = ""
        while (line != null && lineNo < firstDataRowNo) {
            if(rowDelimiter.equals("\r")||rowDelimiter.equals("\n")||rowDelimiter.equals("\r\n")){
                line= br.readLine()
                logDebug("skip line " + line)
            }else{
                line= readRecord(br)
            }

            lineNo += 1
        }
    }

    def readRecord(br: BufferedReader): String = {
        if (!_list.isEmpty) {
            val line = _list.getFirst
            _list.removeFirst()
            line
        } else {
            if (generateDocRecords(br)) {
                readRecord(br)
            } else {
                null.asInstanceOf[String]
            }
        }
    }

    def generateDocRecords(br: BufferedReader): Boolean = {
        val rowDelimiterPattern: Pattern = Pattern.compile(rowDelimiter)
        var buf: Array[Char] = new Array[Char](_bufferSize)
        var bufferNum = 0;
        var len:Int = 0
        len = br.read(buf)
        while(len >= 0) {   // 存在值
            bufferNum += 1
            // process
            _record += String.valueOf(buf, 0, len)
            var flag: Boolean = false
            var start: Int = 0
            val m: Matcher = rowDelimiterPattern.matcher(_record)
            while (m.find) {
                val tmp: String = _record.substring(start, m.start)
                if (org.apache.commons.lang.StringUtils.isNotEmpty(tmp) &&
                        ((RowDelimiterPosition.Begin == position && _count != 0) ||
                                RowDelimiterPosition.End == position)) {
                    _list.add(tmp)
                }
                start = m.end
                flag = true
                _count += 1
            }
            if (flag) _record = _record.substring(start)

            if (bufferNum >= _recordBufferNum) {
                // 当取取的buffer数大于等于 最大值时，就不在取乐
//                len = -2
              if (bufferNum == _recordBufferNum) {
                len = -1
              }  else {
                throw new RuntimeException("读取文件异常")
              }
            } else {
                buf = new Array[Char](_bufferSize)
                len = br.read(buf)
            }

        }
        /**
        *while (br.read(buf) != -1) {
            *_record += String.valueOf(buf).replace("\u0000", "")
            *var flag: Boolean = false
            *var start: Int = 0
            *val m: Matcher = rowDelimiterPattern.matcher(_record)
            *while (m.find) {
                *val tmp: String = _record.substring(start, m.start)
                *if (org.apache.commons.lang.StringUtils.isNotEmpty(tmp) &&
                        *((RowDelimiterPosition.Begin == position && _count != 0) ||
                                *RowDelimiterPosition.End == position)) {
                    *_list.add(tmp)
                *}
                *start = m.end
                *flag = true
                *_count += 1
            *}
            *if (flag) _record = _record.substring(start, _record.length)
            *buf = new Array[Char](_bufferSize)
            *if (_list.size >= _recordSize) {
                *return true
            *}
        *}*/
        // 对最后一行的处理 len==-1 最后一次读取，record不为空，则添加为最后一行
        if (len == -1 && org.apache.commons.lang.StringUtils.isNotBlank(_record)) {
            _list.add(_record)
            _record = null
        }
        _list.size != 0
    }
}
