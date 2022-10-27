package com.eurlanda.datashire.engine.spark

import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by zhudebin on 16/9/18.
  */
object DataFrameTest {

  def genSS(): SparkSession = {
    SparkSession.builder().master("local[4]").appName("testDataFrame").getOrCreate()
  }

  def genUserDF(ss: SparkSession): DataFrame = {
    val data = (0 to 1000).map(i => (i, s"name$i", i % 80, s"addr$i", i % 5))

    val rdd = ss.sparkContext.parallelize(data).map(t5 => Row(t5._1, t5._2, t5._3, t5._4, t5._5))

    ss.createDataFrame(rdd, StructType(Seq(StructField("id", types.IntegerType, false),
      StructField("name", types.StringType, false),
      StructField("age", types.IntegerType, false),
      StructField("addr", types.StringType, false),
      StructField("depId", types.IntegerType, false)
    )))
  }

  def genDepDF(ss: SparkSession): DataFrame = {
    val data = (0 to 4).map(i => (i, s"dep$i"))

    val rdd = ss.sparkContext.parallelize(data).map(t2 => Row(t2._1, t2._2))

    ss.createDataFrame(rdd, StructType(Seq(StructField("id", types.IntegerType, false),
      StructField("name", types.StringType, false)
    )))
  }

  def main(args: Array[String]) {
    testAlias(genSS())
  }

  def testJoin(ss: SparkSession): Unit = {
    val dep = genDepDF(ss).alias("dep")
    val user = genUserDF(ss).alias("user")

    val df = dep.join(user, "id")

    df.printSchema()

    df.select("dep.name").show()

  }

  def testAlias(ss: SparkSession): Unit = {
    val dep = genDepDF(ss).alias("dep")
    val user = genUserDF(ss).alias("user")

    val df = dep.join(user, "id").cache()

    df.printSchema()

    val df2 = df.selectExpr("id", "age", "user.name as uname", "dep.name as dname", "depId").as("user")

//    df2.join(dep, new Column(Utils.sqlToExpression("user.depId = dep.id", ss))).show()

    df2.createOrReplaceTempView("user")

    ss.sql("use default")
    ss.sql("show tables").show()
  }

  /**
    * def testMap(ss: SparkSession): Unit = {
    * val dep = genDepDF(ss).alias("dep")
    * val user = genUserDF(ss).alias("user")

    * dep.map(row => {
    * row.anyNull
    * })

    * val df = dep.join(user, "id").cache()

    * df.printSchema()

    * val df2 = df.selectExpr("id", "age", "user.name as uname", "dep.name as dname", "depId").as("user")

    * //    df2.join(dep, new Column(Utils.sqlToExpression("user.depId = dep.id", ss))).show()

    * df2.createOrReplaceTempView("user")

    * ss.sql("use default")
    * ss.sql("show tables").show()
    * }  */

  def genStructType():StructType = {
    StructType(Seq(
      StructField("11hphm", StringType, true),
      StructField("11xsfx", StringType, true),
    StructField("11kkbh", StringType, true)
    ))
  }
}
