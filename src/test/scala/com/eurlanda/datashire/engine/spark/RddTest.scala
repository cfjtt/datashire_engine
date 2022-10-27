package com.eurlanda.datashire.engine.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhudebin on 2017/5/5.
  */
object RddTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[4]").setAppName("test")

    val spark = new SparkContext(conf)

    val max = spark.parallelize(Seq[Short](1, 100)).max()(new Ordering[Short]() {
      override
      def compare(x: Short, y: Short): Int = {
        y-x
      }
    })

    println(s"--------------------- $max")
  }

}
