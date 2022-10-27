package com.eurlanda.datashire.engine.spark.mllib

import java.util
import java.util.{List => JList, Map => JMap}

import com.eurlanda.datashire.engine.entity.{DataCell, TDataType}
import com.eurlanda.datashire.engine.spark.mllib.model.DiscretizeModel
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.storage.StorageLevel


/**
 * Created by zhudebin on 14-6-5.
 */
class DiscretizeSquid( preRDD:JavaRDD[JMap[Integer, DataCell]],
                      inKey: Integer,
                      buckets: Integer) extends Serializable {
    def run(): (DiscretizeModel, java.lang.Float, java.lang.Long) = {
        val list = preRDD.rdd.persist(StorageLevel.MEMORY_AND_DISK)
        val resultMap = list.reduce((m1:JMap[Integer,DataCell], m2:JMap[Integer, DataCell]) => {
            var min = 0.0d
            var max = 0.0d
            var min1,min2,max1,max2 = 0.0d
            if(m1.get(inKey).getdType() == TDataType.STRING) {
                val arr1 = m1.get(inKey).getData.toString.split(",")
                min1 = arr1(0).toDouble
                max1 = arr1(1).toDouble
                if(m2.get(inKey).getdType() == TDataType.STRING) {
                    val arr2 = m2.get(inKey).getData.toString.split(",")
                    min2 = arr2(0).toDouble
                    max2 = arr2(1).toDouble
                } else {
                    min2 = m2.get(inKey).getData.toString.toDouble
                    max2 = min2
                }
            } else {
                min1 = m1.get(inKey).getData.toString.toDouble
                max1 = min1
                if(m2.get(inKey).getdType() == TDataType.STRING) {
                    val arr2 = m2.get(inKey).getData.toString.split(",")
                    min2 = arr2(0).toDouble
                    max2 = arr2(1).toDouble
                } else {
                    min2 = m2.get(inKey).getData.toString.toDouble
                    max2 = min2
                }
            }
            min = if(min1>min2) min2 else min1
            max = if(max1>max2) max1 else max2
            val map = new util.HashMap[Integer, DataCell]()
            map.put(inKey, new DataCell(TDataType.STRING, min + "," + max))
            map
        })
       // outRDD=resultMap;

        var min = 0.0d
        var max = 0.0d
        // 说明整个RDD只有一行数据
        if(resultMap.get(inKey).getdType() != TDataType.STRING) {
           // min =  resultMap.get(inKey).getData.asInstanceOf[Double]
          //  max = resultMap.get(inKey).getData.asInstanceOf[Double]
            min = resultMap.get(inKey).getData.toString.toDouble
            max = resultMap.get(inKey).getData.toString.toDouble
        } else {
            val arr = resultMap.get(inKey).getData.toString.split(",")
            min = arr(0).toDouble
            max = arr(1).toDouble
        }
        val count = list.count()
        list.unpersist(blocking = false)
        (new DiscretizeModel(min,max, buckets),1.0f,count)
    }


}
