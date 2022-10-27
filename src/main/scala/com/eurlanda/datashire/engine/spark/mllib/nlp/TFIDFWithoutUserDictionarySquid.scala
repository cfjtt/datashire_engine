package com.eurlanda.datashire.engine.spark.mllib.nlp

import java.util

import com.eurlanda.datashire.engine.entity.{DataCell, TDataType, TTransformationSquid}
import com.eurlanda.datashire.engine.service.SquidFlowLauncher
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.immutable.List
import scala.collection.mutable.ArrayBuffer



/**
  * Created by Administrator on 2016-06-12.
  * 用户不指定词典，用系统自带词典
  */
class TFIDFWithoutUserDictionarySquid(preRDD: RDD[util.Map[Integer, DataCell]],
                                       inKey: Integer,
                                       outKey: Integer) extends Serializable {

  /**
    * 判断是否是异常数据
    * @param m
    * @return
    */
  private def isErrorData(m: java.util.Map[Integer, DataCell]):Boolean={
    if (m.get(inKey) == null || m.get(inKey).getData == null ) {
      m.put(TTransformationSquid.ERROR_KEY, new DataCell(TDataType.STRING, "异常数据"))
      true
    } else if(m.get(inKey).getData.toString.length == 0) { // 空字符串
      m.put(TTransformationSquid.ERROR_KEY, new DataCell(TDataType.STRING,"不能为空字符串") )
      true
    } else {
      m.containsKey(0)
    }
  }

  /**
    *
    * @param sparkContext
    * @return
    */
  def run(sparkContext: SparkContext)  = {
      preRDD.persist(StorageLevel.DISK_ONLY)
      // 异常数据
      // val errorRdd = preRDD.filter((m: java.util.Map[Integer, DataCell]) => isErrorData(m))
      // 正常数据, 需要去掉停用词吗？
      val rdd = preRDD.filter(x => !isErrorData(x))
      val resultRDD = preRDD.collect().map(xmap => {
        if (isErrorData(xmap)) {
          xmap
        } else {
          val words = getSegmentWordsWithoutUserDictionary(xmap.get(inKey).getData.toString).distinct //分词
          var tmpmaprdd: RDD[Vector] = null
          tmpmaprdd = getTFIDF(sparkContext, words)
          val restmp = tmpmaprdd.reduce(vectorAddVector) //一个句子形成一个vector
          val map: util.Map[Integer, DataCell] = new util.HashMap[Integer, DataCell]()
          map.put(outKey, new DataCell(TDataType.CSN, restmp.toArray.mkString(",")))
          map.putAll(xmap) // 加入源数据，否则源数据列导出时会丢失
          map
        }
      })
      preRDD.unpersist()
      sparkContext.makeRDD(resultRDD)
  }

  def vectorAddVector(v1:Vector,v2:Vector): Vector = {
    val res = new ArrayBuffer[Double]()
    for (i <- 0.until(v1.toDense.values.length)) {
      res.append(v1.apply(i) + v2.apply(i))
    }
    Vectors.dense(res.toArray)
  }

  /**
    * TFIDF算法
    *
    * @param sparkContext
    * @param words 分词完毕后的词
    * @return
    */
  private def getTFIDF(sparkContext: SparkContext, words : List[String]): RDD[Vector] = {
    import org.apache.spark.mllib.feature.{HashingTF, IDF}
    import org.apache.spark.mllib.linalg.Vector
    import org.apache.spark.rdd.RDD

    val documents: RDD[Seq[String]] = sparkContext.makeRDD(words.map(x => Seq(new String(x))))
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    tfidf.map(x=>x.toDense)
  }

  /**
    *FEATURE_SELECTION 算法
    *
    * @param sc
    * @param words  分词完毕后的词,（单词）
    * @return  （tfidf值）
    */
  private def getTFIDFByFeatures(sc:SparkContext, words:List[(Int,String)]) :RDD[Vector] = {
   // val sqlContext = new SQLContext(sc)
   // val sqlContext = SparkSession.builder().config(sc.getConf).getOrCreate();
    val sqlContext = SquidFlowLauncher.getSparkSession
    val sentenceData = sqlContext.createDataFrame(words).toDF("label", "words")
    val tokenizer = new Tokenizer().setInputCol("words").setOutputCol("tokenizer")
    val wordsData = tokenizer.transform(sentenceData)
    val hashingTF = new HashingTF().setInputCol("tokenizer").setOutputCol("rawFeatures") //.setNumFeatures(1024)
    val featurizedData = hashingTF.transform(wordsData)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("features").rdd.map(row => Vectors.parse(row.get(0).toString).toDense)
  }

  /**
    * 用户不用自定义词典，用系统自带的词典
    *
    * @param str
    * @return
    */
 private def getSegmentWordsWithoutUserDictionary(str: String):List[String] = {
    ToAnalysis.parse(str).asScala.map(x=>x.getName).toList
  }

}
