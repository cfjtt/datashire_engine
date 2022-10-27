package com.eurlanda.datashire.engine.squid

import java.util

import com.eurlanda.datashire.engine.ud.UserDefinedSquid
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by zhudebin on 2017/4/19.
  */
object TestDemoUDS {

  def main(args: Array[String]) {

    val config = new SparkConf().setMaster("local[4]").setAppName("test demo uds")

    val spark = SparkSession.builder().config(config).getOrCreate()


    val params = new util.HashMap[String, String]()
    params.put("split", "__++__")

    val uds = Class.forName("com.eurlanda.datashire.engine.squid.Demo1UserDefinedSquid").newInstance().asInstanceOf[UserDefinedSquid]
    uds.setParams(params)

    val inSchema = uds.inputSchema

    val outSchema = uds.outSchema

    val inDS = spark.range(1l, 100l).map(l => {
      Row(l.toInt, l.toInt % 10)
    })(RowEncoder(inSchema))

    val oDS = uds.process(inDS)

    oDS.schema.printTreeString()
    val list = oDS.collect()
    for(o <- list) {
      println(s"------$o--")
    }

    spark.close()
  }

}
