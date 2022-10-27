package com.eurlanda.datashire.engine.spark

import java.util.{ArrayList, HashMap, Map}

import com.eurlanda.datashire.engine.entity.{TStructField, DataCell, TDataType, TSquid}
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid
import com.eurlanda.datashire.enumeration.JoinType
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zhudebin on 15/10/28.
 */
object TJoinSquidTest {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("SJoinSquid test"))
    val path = "/Users/zhudebin/soft/ds/cdh5.4.7/spark-1.5.1-bin-hadoop2.6/examples/src/main/resources/"

    // squid name  p1
    val rdd1:RDD[Map[Integer, DataCell]] = sc.textFile(path + "people.txt").map(_.split(",")).map(p => {
      val map = new HashMap[Integer, DataCell]()

      map.put(1, new DataCell(TDataType.STRING, p(0)))
      map.put(2, new DataCell(TDataType.INT, p(1).trim.toInt))

      map
    })

    // squid name p2
    val rdd2:RDD[Map[Integer, DataCell]] = sc.textFile(path + "people.txt").map(_.split(",")).map(p => {
      val map = new HashMap[Integer, DataCell]()

      map.put(3, new DataCell(TDataType.STRING, p(0)))
      map.put(4, new DataCell(TDataType.INT, p(1).trim.toInt))

      map
    })

    // squid name p3
    val rdd3:RDD[Map[Integer, DataCell]] = sc.textFile(path + "people.txt").map(_.split(",")).map(p => {
      val map = new HashMap[Integer, DataCell]()

      map.put(3, new DataCell(TDataType.STRING, p(0)))
      map.put(4, new DataCell(TDataType.INT, p(1).trim.toInt))

      map
    })
    // squid name p4
    val rdd4:RDD[Map[Integer, DataCell]] = sc.textFile(path + "people.txt").map(_.split(",")).map(p => {
      val map = new HashMap[Integer, DataCell]()

      map.put(3, new DataCell(TDataType.STRING, p(0)))
      map.put(4, new DataCell(TDataType.INT, p(1).trim.toInt))

      map
    })

    val i2c1:Map[Integer, TStructField] = new HashMap[Integer, TStructField]()
    i2c1.put(1, new TStructField("name", TDataType.sysType2TDataType(SystemDatatype.NVARCHAR.value()), true, null, null))
    i2c1.put(2, new TStructField("age", TDataType.sysType2TDataType(SystemDatatype.INT.value()), true, null, null))
    val i2c2 = new HashMap[Integer, TStructField]()
    i2c2.put(3, new TStructField("name", TDataType.sysType2TDataType(SystemDatatype.NVARCHAR.value()), true, null, null))
    i2c2.put(4, new TStructField("age", TDataType.sysType2TDataType(SystemDatatype.INT.value()), true, null, null))
    val i2c3 = new HashMap[Integer, TStructField]()
    i2c3.put(5, new TStructField("name", TDataType.sysType2TDataType(SystemDatatype.NVARCHAR.value()), true, null, null))
    i2c3.put(6, new TStructField("age", TDataType.sysType2TDataType(SystemDatatype.INT.value()), true, null, null))
    val i2c4 = new HashMap[Integer, TStructField]()
    i2c4.put(7, new TStructField("name", TDataType.sysType2TDataType(SystemDatatype.NVARCHAR.value()), true, null, null))
    i2c4.put(8, new TStructField("age", TDataType.sysType2TDataType(SystemDatatype.INT.value()), true, null, null))

    val rdds = new java.util.ArrayList[RDD[Map[Integer, DataCell]]]()
    rdds.add(rdd1)
    rdds.add(rdd2)
    rdds.add(rdd3)
    rdds.add(rdd4)

    val preTSquids = new java.util.ArrayList[TSquid]()
    val t1 = new TSquid {
      override protected def run(jsc: JavaSparkContext): AnyRef = {
        null
      }
    }
    t1.setOutRDD(rdd1.toJavaRDD())
    val t2 = new TSquid {
      override protected def run(jsc: JavaSparkContext): AnyRef = {
        null
      }
    }
    t2.setOutRDD(rdd2.toJavaRDD())
    val t3 = new TSquid {
      override protected def run(jsc: JavaSparkContext): AnyRef = {
        null
      }
    }
    t3.setOutRDD(rdd3.toJavaRDD())
    val t4 = new TSquid {
      override protected def run(jsc: JavaSparkContext): AnyRef = {
        null
      }
    }
    t4.setOutRDD(rdd4.toJavaRDD())
    preTSquids.add(t1)
    preTSquids.add(t2)
    preTSquids.add(t3)
    preTSquids.add(t4)

    val i2cs = new java.util.ArrayList[Map[Integer, TStructField]]()
    i2cs.add(i2c1)
    i2cs.add(i2c2)
    i2cs.add(i2c3)
    i2cs.add(i2c4)

    val squidNames = new ArrayList[String]()
    squidNames.add("p1")
    squidNames.add("p2")
    squidNames.add("p3")
    squidNames.add("p4")

    val joinTypes = new ArrayList[JoinType]()
    joinTypes.add(JoinType.BaseTable)
    joinTypes.add(JoinType.InnerJoin)
    joinTypes.add(JoinType.LeftOuterJoin)
    joinTypes.add(JoinType.LeftOuterJoin)

    val joinExpressions = new ArrayList[String]()
    joinExpressions.add("")
    joinExpressions.add("p1.name=p2.name")
    joinExpressions.add("p1.age>p3.age and p1.name is not null")
    joinExpressions.add("p2.name=p1.name and p4.age < p1.age")

    val sjs1 = new TJoinSquid(preTSquids, joinTypes, joinExpressions, squidNames, i2cs)
    sjs1.run(new JavaSparkContext(sc))

    val list1:Array[Map[Integer, DataCell]] = sjs1.getOutRDD.rdd.collect()

    //val sjs2 = new SJoinSquid(lrdd, rrdd, JoinType.FullJoin, "p1.age > p2.age", "p2", "p1", li2c, ri2c)
    //sjs2.run(new JavaSparkContext(sc))
    //val list2:Array[Map[Integer, DataCell]] = sjs2.getOutRDD.rdd.collect()
    printlnResult(list1, i2cs)
    println("-----------------------------------------------")
    //printlnResult(list2, li2c, ri2c)

  }

  def printlnResult(list:Array[Map[Integer, DataCell]], i2cs:java.util.List[Map[Integer, TStructField]]): Unit = {
    println()
    println("-----------------标题------------------------------")
    import scala.collection.JavaConversions._

    for(i2c <- i2cs.toList) {
      val seq = i2c.keySet().toArray(Array[Integer]()).toSeq
      for(id<-seq) {
        print(i2c.get(id).getName + "\t\t")
      }
    }
    println()
    list.map(m => {

      for(i2c <- i2cs.toList) {
        val seq = i2c.keySet().toArray(Array[Integer]()).toSeq
        for(id<-seq) {
          print(m.get(id).getData + "\t\t")
        }
      }
      println()
    })
  }
}
