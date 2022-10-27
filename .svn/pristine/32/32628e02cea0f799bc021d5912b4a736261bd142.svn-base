package com.eurlanda.datashire.engine.spark

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

/**
 * join partitioner
 * Created by Juntao.Zhang on 2014/4/21.
 */
class JoinCustomPartitioner1(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => nonNegativeMod(key.hashCode, numPartitions)
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  override def equals(other: Any): Boolean = other match {
    case c: JoinCustomPartitioner1 =>
      c.numPartitions == numPartitions
    case _ => false
  }
}

object JoinCustomPartitioner1 {

  /**
   * note:Juntao.Zhang spark 分片规则 upstream partitions
   * 如果rdd集合中已经存在有rdd已经设置分区器，
   * 由大到小的优先级选定存在分区器的rdd的分区器进行分区,
   * 同时这个也看出分区器的分区数会由那个rdd的分区数进行决定了
   */
  def getPartitioner(rdd: RDD[_], other: RDD[_], defaultNumPartitions: java.lang.Integer): Partitioner = {
    if (defaultNumPartitions == null) {
      defaultPartitioner(rdd, other)
    } else {
      new JoinCustomPartitioner1(defaultNumPartitions)
    }
  }

  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    val bySize = (Seq(rdd) ++ others).sortBy(_.partitions.size).reverse
    new JoinCustomPartitioner1(bySize.head.partitions.size)
  }
}
