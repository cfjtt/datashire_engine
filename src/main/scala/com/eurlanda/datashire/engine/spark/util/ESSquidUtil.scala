package com.eurlanda.datashire.engine.spark.util

import java.util.{Map => JMap}

import com.eurlanda.datashire.engine.entity.DataCell
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.elasticsearch.spark.ESSparkUtil

/**
 * Created by zhudebin on 15-10-12.
 */
object ESSquidUtil {
  def saveToES(sc:SparkContext, path:String,
               es_nodes:String, es_port:String,
               preRDD: JavaRDD[JMap[Integer, DataCell]],
               id2name:JMap[Integer, String], is_mapping_id: String = null): Unit = {
//    val bin = sc.broadcast(id2name)
    val rdd = preRDD.rdd.map(m => {
      var map = Map[String, Any]()
      val iter = id2name.keySet().iterator()
      while(iter.hasNext) {
        val key = iter.next()
        val name = id2name.get(key)
        // map 添加元素
        map += (name -> m.get(key).getData)
      }
      map
    })
    if(is_mapping_id == null) {
      ESSparkUtil.saveToEs(rdd, path, Map("es.nodes" -> es_nodes,
        "es.port" -> es_port)
//        ConfigurationOptions.ES_NODES_DISCOVERY -> "false",
//        ConfigurationOptions.ES_NODES_CLIENT_ONLY -> "true")
      )
    } else {
      ESSparkUtil.saveToEs(rdd, path, Map("es.nodes" -> es_nodes, "es.port" -> es_port, "es.mapping.id" -> is_mapping_id))
    }
  }
}
