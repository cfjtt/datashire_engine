package org.apache.spark.mllib.recommendation

import java.util.{Map => JMap}

import com.eurlanda.datashire.engine.entity.{TTransformationSquid, DataCell, TDataType}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by zhudebin on 14-6-27.
 */
class ALSTrainModel(val productFeatures:Array[(Int, Array[Double])],
                    val userFeatures:Array[(Int, Array[Double])], rank:Int) extends Serializable {

    var isInit:Boolean = false
    var pfRDD:RDD[(Int, Array[Double])] = null
    var ufRDD:RDD[(Int, Array[Double])] = null
    var model:MatrixFactorizationModel = null

    def init(sc:SparkContext) {
        pfRDD = sc.parallelize(productFeatures)
        ufRDD = sc.parallelize(userFeatures)
        model = new MatrixFactorizationModel(rank, ufRDD, pfRDD)
        isInit = true
    }

    def this(model:MatrixFactorizationModel)={
        this(model.productFeatures.collect(),
            model.userFeatures.collect(),
            model.rank)
    }

    /**
      * 预测 userId, itemId 的得分
      * @param usersProducts
      * @return
      */
    def predict(usersProducts: RDD[(Int, Int)]): RDD[Rating] = {
        if(isInit) {
            model.predict(usersProducts)
        } else {
            throw new RuntimeException("ALS 模型没有初始化，请先初始化")
        }
    }

    /**
      * 预测 userId, itemId 的得分
      * @param usersProducts
      * @param inKey
      * @param outKey
      * @return
      */
    def predict(usersProducts: JavaRDD[JMap[Integer, DataCell]], inKey: Integer, outKey: Integer): JavaRDD[JMap[Integer, DataCell]] = {
        val preRDD = usersProducts //usersProducts.rdd.persist(StorageLevel.MEMORY_AND_DISK_2)
        try {
            // user, product
            // 征用一下0的位置， 把0的和 非零分开
            val errorRDD = preRDD.filter((m: JMap[Integer, DataCell]) => {
                if (m.containsKey(0)) {
                    true
                } else if (m.get(inKey) == null || m.get(inKey).getData == null) {
                    m.put(TTransformationSquid.ERROR_KEY, new DataCell(TDataType.STRING, "ALS 预测数据不能为空"))
                    true
                } else {
                    false
                }
            }).persist(StorageLevel.MEMORY_AND_DISK)
            val normalRDD = preRDD.filter((m: JMap[Integer, DataCell]) => {
                if (!m.containsKey(0)) {
                    val csn = m.get(inKey).getData.toString
                    val ups = csn.split(",")
                    m.put(0, new DataCell(TDataType.ARRAY, (ups(0).toInt, ups(1).toInt)))
                    true
                } else {
                    false
                }
            }).persist(StorageLevel.MEMORY_AND_DISK)
            val predictions: RDD[Rating] = predict(normalRDD.map((row: JMap[Integer, DataCell]) => {
                row.get(0).getData.asInstanceOf[(Int, Int)]
            }))

            val resultRDD = normalRDD.map[((Int, Int), JMap[Integer, DataCell])]((row: JMap[Integer, DataCell]) => {
                (row.get(0).getData.asInstanceOf[(Int, Int)], row)
            }).leftOuterJoin(predictions.map[((Int, Int), Double)] { x =>
                ((x.user, x.product), x.rating)
            }).distinct().values.map[JMap[Integer, DataCell]](row => {   // 不知道为什么这里的leftOuterJoin会导致数据有重复变多，需要去重
                val jmap = row._1   // Map[Integer, DataCell]
                jmap.remove(0)
                jmap.put(outKey, new DataCell(TDataType.DOUBLE, row._2.getOrElse(0.0d)))
                jmap
            }).union(errorRDD).toJavaRDD()
            resultRDD
        } catch {
            case e: Throwable => {
                val errorMsg = e.getMessage
                if (errorMsg.contains("For input string")) {
                   throw new RuntimeException("数据类型不满足要求：第一个元素是整数，第二个元素是整数")
                }
                throw e
            }
        }
    }
}
