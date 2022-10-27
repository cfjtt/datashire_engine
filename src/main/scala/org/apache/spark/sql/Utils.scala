package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SessionState

/**
  * Created by zhudebin on 16/7/5.
  */
object Utils {

  def sqlToExpression(sql: String, sparkSession: SparkSession):Expression = {
    sparkSession.sessionState.sqlParser.parseExpression(sql)
  }

  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
    // scalastyle:on classforname
  }

  /**
    * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
    * loaded Spark.
    *
    * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
    * active loader when setting up ClassLoader delegation chains.
    */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /**
    * Get the ClassLoader which loaded Spark.
    */
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  def genColumn(column:String):Column = {
    Column(column)
  }

  def getSessionState(sparkSession: SparkSession):SessionState = {
    sparkSession.sessionState
  }
}
