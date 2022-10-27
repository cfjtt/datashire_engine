package com.eurlanda.datashire.engine.spark

import com.esotericsoftware.kryo.Kryo
import com.eurlanda.datashire.engine.entity.DataCell
import org.apache.phoenix.execute.ScanPlan
import org.apache.spark.serializer.KryoRegistrator

/**
 * Created by zhudebin on 15-1-12.
 */
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[DataCell])
    kryo.register(classOf[ScanPlan])
    kryo.register(classOf[PhoenixRDDPartition])
//    kryo.register(classOf[Scan])
  }
}
