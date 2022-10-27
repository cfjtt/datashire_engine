package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time

import scala.reflect.ClassTag

/**
  * Created by zhudebin on 16/3/14.
  */
object TransformedDStreamUtil {

  def transform[U: ClassTag](parents: Seq[DStream[_]],
                   transformFunc: (Seq[RDD[_]], Time) => RDD[U]): DStream[U] = {
    val ssc = parents.head.ssc
    ssc.withScope {
      // because the DStream is reachable from the outer object here, and because
      // DStreams can't be serialized with closures, we can't proactively check
      // it for serializability and so we pass the optional false to SparkContext.clean
      val cleanedF = ssc.sparkContext.clean(transformFunc, false)
      val realTransformFunc =
        (rdds: Seq[RDD[_]], time: Time) => {
        assert(rdds.length > 0, "rdds.length : " + rdds.length)
        cleanedF(rdds, time)
      }
      new TransformedDStream[U](parents, realTransformFunc)
    }
  }
}
