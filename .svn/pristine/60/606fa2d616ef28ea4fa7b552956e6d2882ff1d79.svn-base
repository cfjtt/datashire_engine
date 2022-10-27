package org.apache.spark

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.JsonProtocol
import org.json4s.jackson.JsonMethods._

/**
  * Created by zhudebin on 16/5/11.
  */
object ConvertUtils {

  def disableOutputSpecValidation(): Unit = {
    PairRDDFunctions.disableOutputSpecValidation.value_=(true)
  }

  def logEventToString(event: SparkListenerEvent):String = {
    val eventJson = JsonProtocol.sparkEventToJson(event)
    compact(render(eventJson))
  }
}
