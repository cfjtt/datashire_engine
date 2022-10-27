package com.eurlanda.datashire.engine.spark

import com.eurlanda.datashire.enumeration.JoinType
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}

object JoinCustomPartitioner {

  /**
   * note:Juntao.Zhang spark 分片规则 upstream partitions
   * 如果rdd集合中已经存在有rdd已经设置分区器，
   * 由大到小的优先级选定存在分区器的rdd的分区器进行分区,
   * 同时这个也看出分区器的分区数会由那个rdd的分区数进行决定了
   */
  def getPartitioner(rdd: RDD[_], other: RDD[_], defaultNumPartitions: java.lang.Integer, joinType: JoinType): Partitioner = {
    if (defaultNumPartitions == null) {
      defaultPartitioner(rdd, joinType, other)
    } else {
      new HashPartitioner(defaultNumPartitions)
    }
  }

    /**
     * 目前join只可能存在两个，因为我们现阶段的处理模型join 只有两两join
     * @param rdd
     * @param joinType
     * @param others
     * @return
     */
  def defaultPartitioner(rdd: RDD[_], joinType: JoinType, others: RDD[_]*): Partitioner = {
//    val bySize = (Seq(rdd) ++ others).sortBy(_.partitions.size).reverse
    val bySize = if(rdd.partitions.size==1) 2 else rdd.partitions.size  + (if(others.head.partitions.size==1) 2 else others.head.partitions.size)
    val numPartitions = joinType match {
        case JoinType.LeftOuterJoin =>
            rdd.partitions.size
        case JoinType.RightOuterJoin =>
            others.head.partitions.size
        case JoinType.Unoin =>
            rdd.partitions.size + others.head.partitions.size
        case JoinType.UnoinAll =>
            rdd.partitions.size + others.head.partitions.size
        case JoinType.InnerJoin =>
            bySize
        case JoinType.CrossJoin =>
            bySize
        case _ =>
            throw new RuntimeException("没有该JOIN 类型")
    }
    new HashPartitioner(numPartitions)
  }
}
