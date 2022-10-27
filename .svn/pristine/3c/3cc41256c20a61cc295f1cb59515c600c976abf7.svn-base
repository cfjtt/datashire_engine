package com.eurlanda.datashire.engine.spark

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Cast, BoundReference, GenericRowWithSchema}
import org.apache.spark.sql.catalyst.expressions.objects.{DecodeUsingSerializer, EncodeUsingSerializer}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import scala.reflect._
import scala.util.Random


/**
  * Created by zhudebin on 2017/3/7.
  */
object GroupTagTest extends Serializable {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
    val sc = SparkSession.builder().config(conf).getOrCreate()
    /**
      * object testImplicits extends SQLImplicits {
      * protected override def _sqlContext: SQLContext = sc.sqlContext
      * }
      * import  testImplicits._
      */
    val list = Seq(
      (1, "name1", 15, "男"),
      (2, "name2", 20, "女"),
      (3, "name3", 25, "男"),
      (4, "name3", 22, "男"),
      (5, "name2", 25, "男"),
      (6, "name1", 25, "男"))
    val st = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("sex", StringType, nullable = true))

    )

    import scala.collection.JavaConversions._
    val slist = list.map{p => Row(p._1, p._2, p._3, p._4)}

//    implicit val mapEncoder = org.apache.spark.sql.Encoders.bean(classOf[Row])
//    import sc.implicits._
//    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
//    import org.apache.spark.sql.Encoder




//    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Row]

    val df = sc.createDataFrame(slist, st)

    val df2 = df.repartition(new Column("sex"))
      .sortWithinPartitions(new Column("age").desc)

//      .mapPartitions(_ => Iterator(1))

      .mapPartitions{ iter =>
        val num = Random.nextInt(10);
        var idx = 0;
        var lastV = null;
        iter.map(r => {
          println(num + "-----------------------" + r.toString())
          idx = idx + 1
          new GenericRowWithSchema(Array(r.get(0), r.get(1), r.get(2), r.get(3), idx), genStructType1_2())
        })
      }(org.apache.spark.sql.Encoders.kryo[GenericRowWithSchema])
    df2.printSchema()

    df2.collect().map(println(_))
  }

  def genStructType():StructType = {
    StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("sex", StringType, nullable = true))
//      StructField("tag", StringType, nullable = true),
//      StructField("tag1", StringType, nullable = true))

    )
  }

  def genStructType1_2():StructType = {
    StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("sex", StringType, nullable = true),
            StructField("tag", StringType, nullable = true))
      //      StructField("tag1", StringType, nullable = true))

    )
  }

  def genStructType2():StructType = {
    StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("sex", StringType, nullable = true),
      StructField("tag2", StringType, nullable = true),
      StructField("tag1", StringType, nullable = true))

    )
  }

  def genStructType3():StructType = {
    StructType(Seq(
      StructField("id1", IntegerType, nullable = true),
      StructField("name1", StringType, nullable = true),
      StructField("age1", IntegerType, nullable = true),
      StructField("sex1", StringType, nullable = true)
    ))
  }

  def genExpression(st:StructType) ={
    ExpressionEncoder(
//      schema = new StructType().add("value", BinaryType),
      schema = st,
      flat = false,
      serializer = Seq(
        EncodeUsingSerializer(
          BoundReference(0, ObjectType(classOf[AnyRef]), nullable = true), kryo = true),
        EncodeUsingSerializer(
          BoundReference(1, ObjectType(classOf[AnyRef]), nullable = true), kryo = true),
        EncodeUsingSerializer(
          BoundReference(2, ObjectType(classOf[AnyRef]), nullable = true), kryo = true),
        EncodeUsingSerializer(
          BoundReference(3, ObjectType(classOf[AnyRef]), nullable = true), kryo = true),
        EncodeUsingSerializer(
          BoundReference(4, ObjectType(classOf[AnyRef]), nullable = true), kryo = true)

      ),
      deserializer =
        DecodeUsingSerializer(
          Cast(GetColumnByOrdinal(0, BinaryType), BinaryType),
          ClassTag[GenericRowWithSchema](new GenericRowWithSchema(null, null).getClass),
          kryo = true),
      clsTag = ClassTag[GenericRowWithSchema](new GenericRowWithSchema(null, null).getClass)
    )
  }

  def genExpression2() = {
    ExpressionEncoder[(Int, String, String , Int)]()
  }
}
