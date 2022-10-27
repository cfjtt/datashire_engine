package com.eurlanda.datashire.engine.spark.util

import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient

import scala.collection.JavaConverters._

/**
  * Created by zhudebin on 16/2/1.
  */
object KafkaUtil {

  def getTopics(zk : String): java.util.Collection[String] = {
    ZkUtils.getAllTopics(new ZkClient(zk)).toList.asJavaCollection
  }

}
