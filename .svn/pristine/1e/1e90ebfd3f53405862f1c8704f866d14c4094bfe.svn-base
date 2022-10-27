package com.eurlanda.datashire.engine.spark

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.{List => JList, Map => JMap}

import com.eurlanda.datashire.engine.entity.TColumn
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * hbase rdd
 * Created by Juntao.Zhang on 2014/5/6.
 */
private[spark] class HBaseJdbcPartition(idx: Int, val startRow: Array[Any], val pageSize: Long, val params:JList[AnyRef]) extends Partition {
    override def index = idx

    override def toString = "index:" + idx + ",startRow:" + startRow.mkString("|") + ",pageSize:" + pageSize
}

/**
 *
 * @param sc
 * @param getConnection
 * @param sql
 * @param pageNos
 * @param pageSize
 * @param pageCompute
 * @param mapRow
 * @param columns base columns
 */
class HbaseJdbcRDD[T: ClassTag] (sc: SparkContext,
     getConnection: () => Connection,
     sql: String,
     // 总页数
     pageNos: Int,
     // 每页大小
     pageSize: Long,
     //总条数
     totalRows: Long,
     tableName: String,
     // pageSql
     pageCompute: (PreparedStatement, HBaseJdbcPartition) => _,
     mapRow: (ResultSet) => T,
     columns: Array[TColumn],
     pkColumns: Array[TColumn],
     filter: String,
     params:JList[AnyRef]) extends RDD[T](sc, Nil) {

    require(columns != null && columns.length != 0, "columns is empty!")

    import scala.collection.mutable

    //获取column主键 名称
    private[this] def getKeyNames: Array[String] = {
        val keyNames = new mutable.ArrayBuffer[String]
        pkColumns.foreach {
            value => if (value.isPrimaryKey) keyNames += value.getName
        }
        keyNames.toArray
    }

    //根据ResultSet获得column values
    private[this] def getColumnValue(rs: ResultSet, rowNames: Array[String]): Array[Any] = {
        val result = new mutable.ArrayBuffer[Any]
        rowNames.foreach {
            n =>
                result += rs.getObject(n)
        }
        result.toArray
    }

    override def getPartitions: Array[Partition] = {
        val st = System.currentTimeMillis()
        val conn = getConnection()
        val rowNames = getKeyNames
        require(rowNames != null && rowNames.length != 0, "row names is empty!")
        val columnNamesString = rowNames.map("\"" + _ + "\"").mkString(",")
        def isNotEmpty(_filter: String): Boolean = _filter != null && _filter != ""
        try {
            val pagerSql = "SELECT " + columnNamesString + " FROM " + tableName + " WHERE 1=1 " +  (if (isNotEmpty(filter)) " and " + filter else "") + " order by " + columnNamesString
            println("pagerSql: " + pagerSql + " pageSize: " + pageSize + " pageNos:" + pageNos + " totalRows: " + totalRows)
            val stmt = conn.prepareStatement(pagerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
            val rs: ResultSet = stmt.executeQuery()
            val partitions = new ArrayBuffer[HBaseJdbcPartition]
            var currentPage = 1
            var count = 0
            while (rs.next()) {
                if (count % pageSize == 0) {
                    val par = new HBaseJdbcPartition(currentPage - 1, getColumnValue(rs, rowNames), pageSize, params)
                    println(sql + " : " + par + " , page: " + currentPage)
                    partitions += par
                    if (currentPage == pageNos) {
                        return partitions.toArray
                    }
                    count += 1
                    currentPage += 1
                } else {
                    count += 1
                }
            }
            partitions.toArray
        } finally {
            if (conn != null && !conn.isClosed) {
                conn.close()
            }
            println("partition cost time :" + (System.currentTimeMillis() - st))
        }
    }

    override def compute(thePart: Partition, context: TaskContext) = new NextIterator[T] {
        log.info("execute sql : " + sql)
//        context.addOnCompleteCallback {
//            () => closeIfNeeded()
//        }
      context.addTaskCompletionListener{(taskContext: TaskContext) => closeIfNeeded()}
        val conn = getConnection()
        val part = thePart.asInstanceOf[HBaseJdbcPartition]
        val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        log.info("pager: pageSize:" + part.pageSize + ",startRow:" + part.startRow)
        pageCompute(stmt, part)
        val rs = stmt.executeQuery()

        override def getNext(): T = {
            if (rs.next()) {
                mapRow(rs)
            } else {
                finished = true
                null.asInstanceOf[T]
            }
        }

        override def close() {
            try {
                if (null != rs && !rs.isClosed) rs.close()
            } catch {
                case e: Exception => logWarning("Exception closing resultSet", e)
            }
            try {
                if (null != stmt && !stmt.isClosed) stmt.close()
            } catch {
                case e: Exception => logWarning("Exception closing statement", e)
            }
            try {
                if (null != conn && !stmt.isClosed) conn.close()
                logInfo("closed connection")
            } catch {
                case e: Exception => logWarning("Exception closing connection", e)
            }
        }
    }
}
