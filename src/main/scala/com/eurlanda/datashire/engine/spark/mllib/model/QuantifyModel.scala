package com.eurlanda.datashire.engine.spark.mllib.model

import com.eurlanda.datashire.engine.entity.{DataCell, TDataType}

/**
 * Created by zhudebin on 14-6-7.
 */
class QuantifyModel(val map:Map[Any, Double], val tDataType:TDataType,
                    isIgnored: Boolean) extends Serializable {

    var inverseMap:Map[Double, Any] = _

    def predict(key: Any):Double = {
      val mappingKey =
      if(isIgnored) {
        key
      } else {
        key.toString.toUpperCase
      }
      map.get(mappingKey).getOrElse(0)
    }

    def inverse(key: Double):DataCell = {
        if(inverseMap == null) {
            initInverseMap()
        }
        new DataCell(tDataType, inverseMap.get(key).orNull)
    }

    private def initInverseMap() {
        val im = scala.collection.mutable.Map[Double, Any]()
        for(m <- map) {
            im.put(m._2, m._1)
        }
        inverseMap = im.toMap
    }

}
