package com.eurlanda.datashire.engine.spark.util

import java.sql.{Connection, PreparedStatement}
import java.util.{List => JList, Map => JMap}

import com.eurlanda.datashire.engine.entity.{DataCell, TColumn, TDataSource}
import com.eurlanda.datashire.engine.spark.DatabaseUtils
import com.eurlanda.datashire.enumeration.DataBaseType
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.{SparkContext, TaskContext}

import scala.util.control.NonFatal

/**
  * Created by zhudebin on 16/5/16.
  */
object ImpalaSquidUtil {

  val TCOLUMNS = "_TCOLUMNS_"
  val DATASOURCE = "_DATASOURCE_"

  /**
    * 保存数据到Impala
    *
    * @param sc
    * @param preRDD
    * @param _params key:[tcolumns, datasource]
    * @since 2.0.0
    */
  def saveToImpala(sc:SparkContext, preRDD: JavaRDD[JMap[Integer, DataCell]], _params:JMap[String, Any]): Unit = {
    val params_bc = sc.broadcast(_params)

    val writeToImpala = (context: TaskContext, iter: Iterator[JMap[Integer, DataCell]]) => {

      val params = params_bc.value

      val conn :Connection = genConnection(params)
      // check db
      val supportsTransactions = try {
        conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
          conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()
      } catch {
        case NonFatal(e) =>
          true
      }

      tryWithSafeFinally {
        if (supportsTransactions) {
          conn.setAutoCommit(false) // Everything in the same db transaction.
        }
        val psmt = insertStatement(conn, params)
        while (iter.hasNext) {
          val record = iter.next()
          // save to impala
          setStatement(psmt, record, getTColumnSet(params))
          psmt.addBatch()
        }
        psmt.executeBatch()
        conn.commit()
      } {
        if(conn != null) {
          conn.close()
        }
      }
    }

    sc.runJob(preRDD, writeToImpala)
  }

  def genConnection(params:JMap[String, Any]):Connection = {

    val dbi = new TDataSource()
    dbi.setHost(getDataSource(params).getHost)
    dbi.setDbName(getDataSource(params).getDbName)
    dbi.setType(DataBaseType.IMPALA)
    DatabaseUtils.getConnection(dbi)
  }

  def insertStatement(conn:Connection, params:JMap[String, Any]): PreparedStatement = {
    val insertSql = DatabaseUtils.genInsertSQL(
      getDataSource(params), getTColumnSet(params))

    conn.prepareStatement(insertSql)
  }

  def setStatement(pst: PreparedStatement, record:JMap[Integer, DataCell], columns: Set[TColumn]): Unit = {
    var idx = 1
    columns.toSeq.sortBy(_.getId).foreach(c => {
      TColumn.setValue(record.get(c.getId), pst, idx, c)
      idx += 1
    })
  }

  def getDataSource(params:JMap[String, Any]):TDataSource = {
    params.get(DATASOURCE).asInstanceOf[TDataSource]
  }

  def getTColumnSet(params:JMap[String, Any]):Set[TColumn] = {
    import scala.collection.JavaConversions._
    params.get(TCOLUMNS).asInstanceOf[java.util.Set[TColumn]].toSet
  }

  /**
    * Execute a block of code, then a finally block, but if exceptions happen in
    * the finally block, do not suppress the original exception.
    *
    * This is primarily an issue with `finally { out.close() }` blocks, where
    * close needs to be called to clean up `out`, but if an exception happened
    * in `out.write`, it's likely `out` may be corrupted and `out.close` will
    * fail as well. This would then suppress the original/likely more meaningful
    * exception from the original `out.write` call.
    */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    // It would be nice to find a method on Try that did this
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable =>
          if (originalThrowable != null) {
            originalThrowable.addSuppressed(t)
            throw originalThrowable
          } else {
            throw t
          }
      }
    }
  }
}
