package org.apache.spark.deploy.yarn

import java.security.PrivilegedExceptionAction

import com.eurlanda.datashire.engine.dao.{ApplicationStatusDao, SquidFlowDao}
import com.eurlanda.datashire.engine.enumeration.JobStatusEnum
import com.eurlanda.datashire.engine.util.ConfigurationUtil._
import com.eurlanda.datashire.engine.util.{ConfigurationUtil, ConstantUtil}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.{FinalApplicationStatus, YarnApplicationState}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONObject

/**
  * Created by zhudebin on 16/1/19.
  */
object StreamJobSubmit extends Logging {

  def submit(squidflowId:Int, batchDuration: Long,
             config: java.util.Map[String, String]): String = {

    var configMap = Map(
      "spark.driver.memory" -> "1g",
      "spark.executor.instances" -> "3",
      "spark.executor.memory" -> "1g",
      "spark.executor.cores" -> "1",
      "spark.driver.extraJavaOptions" -> "-XX:PermSize=128M -XX:MaxPermSize=512m",
      "spark.executor.extraJavaOptions" -> "-XX:PermSize=128M -XX:MaxPermSize=512m",
      "spark.yarn.queue" -> "spark")

    config.asScala
      //
//      .filter(t2 => {
//      t2._1.startsWith("spark.")
//    })
      .map(t2 => {
      configMap += (t2._1 -> t2._2)
    })

    logInfo("======= 提交流式作业 ========= config:" + configMap)

    // 是否需要代理用户
    if(StringUtils.isNotEmpty(ConfigurationUtil.getSparkProxyUser)) {
      val proxyUser = UserGroupInformation.createProxyUser(ConfigurationUtil.getSparkProxyUser,
        UserGroupInformation.getCurrentUser())
      try {
        proxyUser.doAs(new PrivilegedExceptionAction[String]() {
          override def run(): String = {
            runJob(squidflowId, batchDuration, configMap)
          }
        })
      } catch {
        case e: Exception =>
          // Hadoop's AuthorizationException suppresses the exception's stack trace, which
          // makes the message printed to the output by the JVM not very helpful. Instead,
          // detect exceptions with empty stack traces here, and treat them differently.
          if (e.getStackTrace().length == 0) {
            // scalastyle:off println
            println(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
            // scalastyle:on println
            throw e
          } else {
            throw e
          }
      }
    } else {
      runJob(squidflowId, batchDuration, configMap)
    }


  }

  def runJob(squidflowId:Int, batchDuration: Long,
             config: Map[String, String]): String = {

//    val depJars1 = Seq("hdfs://ehadoop/eurlanda/spark_lib/mongo-java-driver-3.0.1_e2.jar",
//      "hdfs://ehadoop/eurlanda/spark_lib/zkclient-0.3.jar")




//    val depJars = HdfsUtil.listFiles(ConfigurationUtil.getSparkYarnEngineJarsDir(), "(.*)\\.jar", true)
//
//    logInfo("----------" +depJars.asScala.mkString(","))

    /*
    // 命令行参数,单独提出来,方便知道其中具体的参数
    val args2 = Seq(
      //      "--name","org.apache.spark.examples.SparkPi",
      "--driver-memory",config.get("spark.driver.memory").get,
      "--executor-memory",config.get("spark.executor.memory").get,
      "--num-executors",config.get("spark.executor.instances").get,
      "--executor-cores",config.get("spark.executor.cores").get,
      //      "--queue","spark-hh",
      "--jar", ConfigurationUtil.getSparkJarLocation,
      "--addJars",depJars.asScala.mkString(","),
      "--class", "org.apache.spark.deploy.yarn.StreamJob",
      "--arg", squidflowId.toString,
      "--arg", batchDuration.toString
    )
    */

    System.setProperty("SPARK_YARN_MODE", "true")
    val conf = new SparkConf

    conf.setAppName("datashire streaming - " + squidflowId)
      .set("spark.executor.userClassPathFirst","true")
      .set("spark.driver.userClassPathFirst","true")
      .set("spark.master", "yarn")
      .set("spark.submit.deployMode", "cluster")
      .set("spark.shuffle.service.enabled", "true")
      .set("spark.driver.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=512m ")
      .set("spark.executor.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=512m ")
//      .set("spark.executor.extraLibraryPath", SPARK_EXECUTOR_EXTRALIBRARYPATH)
//      .set("spark.executor.extraClassPath", SPARK_EXECUTOR_EXTRALIBRARYPATH() + "*")
//      .set("spark.driver.extraLibraryPath", SPARK_EXECUTOR_EXTRALIBRARYPATH)
//      .set("spark.driver.extraClassPath", SPARK_EXECUTOR_EXTRALIBRARYPATH() + "*")
      .set("spark.yarn.queue", getSparkYarnQueue)
      .set("spark.yarn.jars", ConfigurationUtil.getSparkYarnEngineJarsDir() + "*")
//      .set("spark.executor.instances", config.get("spark.executor.instances").get)

    // 用户自定义
    config.filter(t2 => {
      t2._1.startsWith("spark.")
    }).foreach(t2 => {
      conf.set(t2._1, t2._2)
    })

    // 设置 jar, class, args
    val args = new ClientArguments(Array[String]())
    args.userJar = ConfigurationUtil.getSparkJarLocation
    args.userClass = "org.apache.spark.deploy.yarn.StreamJob"
    args.userArgs = new ArrayBuffer[String]() +=
      squidflowId.toString += batchDuration.toString

//    if (!Utils.isDynamicAllocationEnabled(conf)) {
//      conf.setIfMissing("spark.executor.instances", args.userArgs + "")
//    }
    logInfo(s"============== 提交流式作业 ============= 启动参数:${conf.toDebugString}")
    logInfo(s"============== 提交流式作业 ============= 启动参数: "
//      s"[driver.memory]-${args.amMemory.toString}," +
//      s"[executor.numExecutors]-${args.numExecutors.toString}," +
//      s"[executor.executorMemory]-${args.executorMemory.toString}," +
//      s"[executor.executorCores]-${args.executorCores.toString}"
    )

    val client = new Client(args, conf)
    val appId = client.submitApplication()
    println("----------- 启动APP ID:" + appId.toString)

    // todo 设置超时,超时则干掉
    val (yarnApplicationState, finalApplicationStatus) = client.monitorApplication(appId, true, true)

    var status:String = null
    if (yarnApplicationState == YarnApplicationState.FAILED ||
      finalApplicationStatus == FinalApplicationStatus.FAILED) {
      status = JobStatusEnum.FAIL.name()
    } else if (yarnApplicationState == YarnApplicationState.KILLED ||
      finalApplicationStatus == FinalApplicationStatus.KILLED) {
      status = JobStatusEnum.KILLED.name()
    } else if(yarnApplicationState == YarnApplicationState.RUNNING) {
      // 提交完成,作业正在运行中
      // 保存状态
      logInfo(s"应用 $appId 提交成功,正在运行中")
      status = JobStatusEnum.RUNNING.name();
    } else {
      status = "UNDEFINED"
      log.error(s"未知状态:$appId")
    }
    logInfo(s"squidflow yarn 运行应用 $appId, 状态为 $status")

    val applicationStatusDao = ConstantUtil.getApplicationStatusDao()
    val squidFlowDao = ConstantUtil.getSquidFlowDao()
    val squidFlowInfo = squidFlowDao.getSquidFlowInfo(squidflowId)
    // 保存 应用的状态 到 数据库
    applicationStatusDao.saveNewApplicationStatus(squidFlowInfo.get("REPOSITORYID").toString.toInt,
      squidFlowInfo.get("PROJECTID").toString.toInt, squidflowId, appId.toString, status, JSONObject(config).toString())

    logInfo(s"======= 启动流式作业成功,applicationId: $appId")

    appId.toString
  }

  /**
    * def main(args: Array[String]) {
    * ConstantUtil.init()
    * submit(363, 500, Map(
    * "spark.driver.memory" -> "1g",
    * "spark.executor.instances" -> "3",
    * "spark.executor.memory" -> "1g",
    * "spark.executor.cores" -> "1",
    * "spark.yarn.queue" -> "spark"))
    * }*/
}
