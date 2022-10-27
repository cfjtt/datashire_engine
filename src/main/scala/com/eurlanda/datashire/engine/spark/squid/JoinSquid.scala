package com.eurlanda.datashire.engine.spark.squid

import java.util.{List => JList, Map}

import com.eurlanda.datashire.engine.entity.DataCell
import com.eurlanda.datashire.engine.spark.JoinCustomPartitioner
import com.eurlanda.datashire.enumeration.JoinType
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

/**
 * Created by zhudebin on 14-8-2.
 */
class JoinSquid(leftRDD:RDD[Map[Integer, DataCell]],
                rightRDD:RDD[Map[Integer, DataCell]],
                leftKeys:JList[Integer], rightKeys:JList[Integer],
                joinType: JoinType) extends Serializable {

    def process():RDD[Map[Integer, DataCell]] = {
        val lr = leftRDD.map[(List[DataCell], Map[Integer, DataCell])](m => {
            val keyGroup = for(i <- leftKeys.toArray) yield {
                m.get(i)
            }
            (keyGroup.toList, m)
        })
        val rr = rightRDD.map[(List[DataCell], Map[Integer, DataCell])](m => {
            val keyGroup = for(i <- rightKeys.toArray) yield {
                m.get(i)
            }
            (keyGroup.toList, m)
        })
        val partitioner: Partitioner = JoinCustomPartitioner.getPartitioner(leftRDD, rightRDD, 10, joinType)
        val resultRDD = joinType match {
            case JoinType.LeftOuterJoin =>
                lr.leftOuterJoin(rr, partitioner)
                        .map[Map[Integer, DataCell]]((p:(List[DataCell],(Map[Integer, DataCell], Option[Map[Integer, DataCell]]))) => {
                    val left = p._2._1
                    val rightOpt = p._2._2
                    if(!rightOpt.isEmpty) {
                        val right = rightOpt.get
                        val keyIter = right.keySet().iterator()
                        while(keyIter.hasNext) {
                            val key = keyIter.next()
                            left.put(key, right.get(key))
                        }
                    }
                    left
                })
            case JoinType.RightOuterJoin =>
                lr.rightOuterJoin(rr, partitioner)
                        .map[Map[Integer, DataCell]]((p:(List[DataCell],(Option[Map[Integer, DataCell]], Map[Integer, DataCell]))) => {
                    val leftOpt = p._2._1
                    val right = p._2._2
                    if(!leftOpt.isEmpty) {
                        val left = leftOpt.get
                        val keyIter = left.keySet().iterator()
                        while(keyIter.hasNext) {
                            val key = keyIter.next()
                            right.put(key, left.get(key))
                        }
                    }
                    right
                })
            case JoinType.InnerJoin =>
                lr.join(rr, partitioner).map(f => {
                    val left = f._2._1
                    val right = f._2._2
                    val keyIter = right.keySet().iterator()
                    while(keyIter.hasNext) {
                        val key = keyIter.next()
                        left.put(key, right.get(key))
                    }
                    left
                })
            /**
            case JoinType.FullJoin =>
                lr.cogroup(rr, partitioner)
                        .map[Map[Integer, DataCell]]((p:(List[DataCell],(Iterable[Map[Integer, DataCell]], Iterable[Map[Integer, DataCell]]))) => {
                    new util.HashMap[Integer, DataCell]()
                    // todo 实现
                })
            case JoinType.CrossJoin =>
                lr.cartesian(rr).map(f => {
                    new util.HashMap[Integer, DataCell]()
                    // todo 实现
                })
              **/
            case _ =>
                throw new RuntimeException("不能处理该类型的Join:" + joinType)
        }
        resultRDD
    }

}
