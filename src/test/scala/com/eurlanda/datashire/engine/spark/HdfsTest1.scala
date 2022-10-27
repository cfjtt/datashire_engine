package com.eurlanda.datashire.engine.spark

import java.util.Date

//import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred._
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by zhudebin on 16/4/19.
  */
object HdfsTest1 {
  val path = "file:///Users/zhudebin/Documents/iworkspace/opensource/bigdata_market/spark/doc/text.txt"

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[6]")
      .setAppName("hdfs test")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
//    val lines = sc.hadoopFile[LongWritable, Text,TextInputFormat]("/Users/zhudebin/Documents/iworkspace/opensource/bigdata_market/spark/doc/text.txt", 2)

//    hdfsSequence(sc)
    pairHdfsText(sc)
  }

  def hdfsText(sc:SparkContext): Unit = {
    val lines = sc.textFile(path, 2)

    val count = lines.count()
    println("count=" + count)
    lines.collect().foreach(line => {
      println(line)
    })


    val lines2 = sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 2)
    val count2 = lines2.count()
    println("count2=" + count2)
    lines2.collect().foreach(t2 => {
      println(t2._1.get().toString + "," + t2._2.toString)
    })

    lines2.foreach(t2 => {
      println("---- " +t2._1.get() + "," + t2._2.toString)
    })

    /**
      * mapreduce.output.textoutputformat.separator  分隔符
      *
      *
      */
//    println("====" +CSVFormat.DEFAULT.format("hello", "aaa\"/aa", "b,\"\"\"\"\"\"\"b", new Date()))
//    println("====" +CSVFormat.EXCEL.format("hello", "aaa\"/aa", "b,\"\"\"\"\"\"\"", new Date()))
//    println("====" +CSVFormat.MYSQL.format("hello", "aaa\"/aa", "b,\"\"\"\"\"\"\"b", new Date()))
//    println("====" +CSVFormat.RFC4180.format("hello", "aaa\"/aa", "b,\"\"\"\"\"\"\"b", new Date()))
//    println("====" +CSVFormat.TDF.format("hello", "aaa\"/aa", "b,\"\"\"\"\"\"\"b", new Date()))
        lines.saveAsTextFile("hdfs://e102:8020/testDir/test1")
  }

  def hdfsSequence(sc:SparkContext) {
    val lines = sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 10)
    lines.saveAsSequenceFile("hdfs://e160:8020/user/ds/test2/test1.seq")
    lines.saveAsObjectFile("")
  }

  def pairHdfsText(sc:SparkContext): Unit = {
    val lines = sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 10)
    val pairRDD = lines.map(t2 => {
      val line = t2._2.toString.split(" ")
      (line(0), line(1))
    })

    // 处理 Append，Overwrite，ErrorIfExists，Ignore
    // 1. Append          1>判断是否有文件夹存在 2> 存在,删除里面 _temp文件夹  3> 设置不需要验证
    // 2. Overwrite       1>删除目标文件夹
    // 3. ErrorIfExists   1>判断是否有文件夹存在 2> 存在 抛异常,squidflow 运行异常
    // 4. Ignore          1>判断是否有文件夹存在 2> 存在, 不执行落地操作

    val processType = 2

    val jobConf = new JobConf(sc.hadoopConfiguration)

    val targetPath = "hdfs://e160:8020/user/ds/test3/test1.txt"

    val continue = prepareSave(processType, jobConf, targetPath)
//    pairRDD.saveAsNewAPIHadoopDataset(conf)

    if(continue) {

    } else {
      return
    }

    implicit def pairRDDToCustomPairRDDFunctions[K, V](pairRDD: RDD[(K, V)])
        (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null):CustomPairRDDFunctions[K, V] = {
      new CustomPairRDDFunctions(pairRDD)
    }


    val hadoopConf = jobConf
    hadoopConf.setOutputKeyClass(classOf[NullWritable])
    hadoopConf.setOutputValueClass(classOf[Text])
    // Doesn't work in Scala 2.9 due to what may be a generics bug
    // TODO: Should we uncomment this for Scala 2.10?
    // conf.setOutputFormat(outputFormatClass)
//    hadoopConf.set("mapred.output.format.class", classOf[TextOutputFormat].getName)
    hadoopConf.set("mapred.output.format.class", "org.apache.hadoop.mapred.TextOutputFormat")
//    for (c <- codec) {
//      hadoopConf.setCompressMapOutput(true)
//      hadoopConf.set("mapred.output.compress", "true")
//      hadoopConf.setMapOutputCompressorClass(c)
//      hadoopConf.set("mapred.output.compression.codec", c.getCanonicalName)
//      hadoopConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)
//    }

    // Use configured output committer if already set
    if (jobConf.getOutputCommitter == null) {
      hadoopConf.setOutputCommitter(classOf[FileOutputCommitter])
    }

    FileOutputFormat.setOutputPath(hadoopConf,
      CustomSparkHadoopWriter.createPathFromString(targetPath, hadoopConf))


//    val jobConf = new JobConf(sc.hadoopConfiguration)

    pairRDD.saveAsHadoopDataset(hadoopConf)
  }

  def prepareSave(processType : Int, conf:Configuration, path:String): Boolean = {

    processType match {
      case 1 => {
        // 1
        val filePath = new Path(path)
        val fileSystem = filePath.getFileSystem(conf)
        val exist = fileSystem.exists(filePath)
        if(exist) {
          if(fileSystem.exists(new Path(path + "/_temporary"))) {
            fileSystem.delete(new Path(path + "/_temporary"), true)
          }
        }
        ConvertUtils.disableOutputSpecValidation()
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        // 2

        // 3
      }
      case 2 => ()
        val filePath = new Path(path)
        val fileSystem = filePath.getFileSystem(conf)
        val exist = fileSystem.exists(filePath)
        if (exist) {
          fileSystem.delete(filePath, true)
        }
      case 3 => ()
        val filePath = new Path(path)
        val fileSystem = filePath.getFileSystem(conf)
        val exist = fileSystem.exists(filePath)
        if(exist) throw new RuntimeException("exit file")
      case 4 => ()
        val filePath = new Path(path)
        val fileSystem = filePath.getFileSystem(conf)
        val exist = fileSystem.exists(filePath)
        if(exist) {
          return false
        }
      case _ => ()
    }

    true
  }

}
