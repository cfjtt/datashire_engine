package com.eurlanda.datashire.engine.spark

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.{List => JList, Map => JMap}

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * jdbc partition  采用数据库分页的方式
 * @param idx
 * @param pageNo pageNo 从1开始
 * @param pageSize 每页大小
 */
class CustomJdbcPartition(idx: Int, val pageNo: Long, val pageSize: Long, val params: JList[AnyRef]) extends Partition {
  override def index = idx
}

/**
 * Created by zhudebin on 14-1-9.
 */
class CustomJdbcRDD[T: ClassManifest](
  sc: SparkContext,
  getConnection: () => Connection,
  sql: String,
  // 总页数
  pageNos:Int,
  // 每页大小
  pageSize:Long,
  // pageSql
  pageCompute:(PreparedStatement,CustomJdbcPartition) => _,
  mapRow: (ResultSet) => T = CustomJdbcRDD.resultSetToObjectArray _,
  val params: java.util.List[AnyRef])
  extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    // 分页SQL 数据分片
    (0 until pageNos).map(i => {
      new CustomJdbcPartition(i, i+1, pageSize, params)
    }).toArray
  }


  override def compute(thePart: Partition, context: TaskContext) = new NextIterator[T] {

    log.info("  execute sql === " + sql)
    context.addTaskCompletionListener{(taskContext: TaskContext) => closeIfNeeded()}
//    context.addOnCompleteCallback{ () => closeIfNeeded() }
    val conn = getConnection()
    val part = thePart.asInstanceOf[CustomJdbcPartition]
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    // setFetchSize(Integer.MIN_VALUE) is a mysql driver specific way to force streaming results,
    // rather than pulling entire resultset into memory.
    // see http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-implementation-notes.html
    if (conn.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt.getFetchSize + " to force MySQL streaming ")
    }
    log.info(" page search ==== pagesize:" + part.pageSize + ",pageNo:" + part.pageNo)
    pageCompute(stmt, part)

    val rs = stmt.executeQuery()

    override def getNext: T = {
      if (rs.next()) {
        mapRow(rs)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }

    override def close() {
      try {
        if (null != rs) rs.close()
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt) stmt.close()
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn && ! conn.isClosed()) conn.close()
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }
}

object CustomJdbcRDD {
    def resultSetToObjectArray(rs: ResultSet) = {
        Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
    }
}
