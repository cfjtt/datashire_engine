package org.apache.spark.sql.hive.thriftserver

import com.eurlanda.datashire.engine.service.SquidFlowLauncher
import com.eurlanda.datashire.engine.util.ConfigurationUtil
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.conf.HiveConf.ConfVars

/**
  * Created by zhudebin on 16/8/29.
  */
object ThriftServer2Util {

//  val log = LogFactory.getLog(classOf[ThriftServer2Util])

  def startThriftServer(): Unit = {
//    System.setProperty(ConfVars.HIVE_SERVER2_THRIFT_PORT.toString, ConfigurationUtil.getThriftServerPort)
//    HiveThriftServer2.startWithContext(SquidFlowLauncher.getSparkSession.sqlContext)
//    log.info("============ init thriftserver success =============")
  }


}
