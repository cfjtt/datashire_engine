package com.eurlanda.datashire.engine.spark.mllib

import java.util.{List => JList}

import com.eurlanda.datashire.engine.entity.DataCell
import com.eurlanda.datashire.engine.spark.mllib.model.QuantifyModel
import com.eurlanda.datashire.engine.util.DSUtil
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by zhudebin on 14-6-5.
 */
class QuantifySquid(preRDD:JavaRDD[java.util.Map[Integer, DataCell]],
                    inKey: Integer,
                    isIgnored: Boolean,
                    min: Double,
                    max: Double) extends Serializable {
    require(min<=max,"QuantifySquid模型中量化区间最小值要小于或等于量化区间最大值")

   def run():(QuantifyModel,java.lang.Float, java.lang.Long) = {
       val rdd = preRDD.rdd.persist(StorageLevel.MEMORY_AND_DISK)
       val total = rdd.count()
       if(total == 0) {
           throw new RuntimeException("生成量化模型异常，训练数据为零")
       }
       val tDataType = rdd.first().get(inKey).getdType()

       val list = rdd.groupBy(m => {
           val dc = m.get(inKey)
           if(isIgnored) {
             dc.getData
           } else {
             if(DSUtil.isNotNull(dc)) {
               dc.getData.toString.toUpperCase
             } else {
               null
             }
           }
       }).map(tuple2 => {
           tuple2._1
       }).collect()

       val m = scala.collection.mutable.Map.empty[Any, Double]
       val part = if(min==0 && max ==0) 1d else ((BigDecimal(max) - min)/(if(list.size==1) 1 else (list.size-1))).toDouble
       var idex = min-part
       list.foreach(str => {
           idex += part
          // if( (!isIgnored) && (tDataType == TDataType.STRING || tDataType == TDataType.CSV)) { // 不区分大小写
           if(isIgnored) { // 区分大小写
             m += (str -> idex)
           } else {
             var idextmp=idex
             if(m.size>0){
               for(key<- m.keySet) {
                 if (key.toString.equalsIgnoreCase(str.toString)) { // 不区分大小写，已经存在key
                   idextmp = m.get(key).get
                 }
                 m += (str -> idextmp)
               }
             }else {
               m += (str -> idextmp)
             }
           }
       })
       rdd.unpersist()
       (new QuantifyModel(m.toMap, tDataType, isIgnored),1.0f, total)
   }
}
