package org.apache.spark.deploy.yarn

import com.eurlanda.datashire.engine.util.ConstantUtil

/**
  * yarn 启动测试类
  *
  * Created by zhudebin on 16/3/25.
  */
object StreamJobYarnTest {

  def main(args: Array[String]) {
//    ConstantUtil.init()
    import scala.collection.JavaConversions._
    // squidflowId:Int, batchDuration: Long
    StreamJobSubmit.submit(348, 500, Map(
      "spark.driver.memory" -> "2g",
      "spark.executor.instances" -> "3",
      "spark.executor.memory" -> "1g",
      "spark.executor.cores" -> "1",
      "spark.yarn.queue" -> "spark"))
  }

}
