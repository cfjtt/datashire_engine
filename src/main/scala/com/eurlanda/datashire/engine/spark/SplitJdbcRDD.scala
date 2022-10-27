package com.eurlanda.datashire.engine.spark

import java.sql.{Connection, PreparedStatement, ResultSet, Types}
import java.util.{List => JList}

import com.eurlanda.datashire.engine.spark.db._
import com.eurlanda.datashire.enumeration.DataBaseType
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag


private [spark] class SplitJdbcPartition(idx: Int, val lower: String, val upper: String, val params: JList[AnyRef]) extends Partition {
  override def index = idx
}

/**
 * 按照指定切片规则切分
 *
 * @param sc sparkContext
 * @param getConnection 生成connection
 * @param sql 查询SQL
 * @param mapNum 分割
 * @param pageCompute
 * @param mapRow
 * @param params
 * @tparam T
 */
class SplitJdbcRDD[T: ClassTag](
     sc: SparkContext,
     getConnection: () => Connection,
     sql: String,
     dbType : DataBaseType,
     tableName: String,
     alias: String,
     splitCol:String,
     splitColType: DbBaseDatatype,
     conditions: String,
     // 分片数
     var mapNum:Int,
     // pageSql
     pageCompute:(PreparedStatement,JList[AnyRef]) => _,
     mapRow: (ResultSet) => T = SplitJdbcRDD.resultSetToObjectArray _,
     val params: JList[AnyRef])
  extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    if(mapNum > 100000) {
      throw new RuntimeException("分区大于" + 100000
        + ",系统暂时不支持,请减少分区数,以便系统能更稳定的运行");
    }
    log.info("-------- 开始获取连接 -----------")

    var con:Connection = null

    try {
      import scala.collection.JavaConversions._
      if (mapNum == 1) {
        val partitions: JList[SplitJdbcPartition] = DBSplitter.splitOnePartition(params)
        partitions.toList.toArray
      } else {
        /**
          * 检查分区列的类型,因为不同数据库有一些类型的列现在还不能当做分区列,会导致抽取的数量不对
          * 放到运行前检查

        def checkPartitionColumnType(): Unit = {
          val splitColInfo = ConfigurationUtil.getSplitColumnInfo

          if(splitColInfo.get(dbType.name()) != null
            && splitColInfo.get(dbType.name()).split(",")
            .contains(splitColType.name().toUpperCase)) {
            throw new RuntimeException(s"数据库${dbType.name()}不能使用${splitColType.name()}类型的${splitCol}作为分片列,如果没有其他列,请选择分片数为1");
          }
        }
        checkPartitionColumnType()
        */
        con = getConnection()
        log.info("-------- 获取连接成功 -----------")
        import DatabaseUtils.getBoundingValsQuery
        val mmSql = getBoundingValsQuery(dbType, splitCol, tableName, conditions, alias)
        log.info("范围变量查询SQL：" + mmSql)
        val pstmt = con.prepareStatement(mmSql)
        log.info("范围变量查询prepareStatement")

        pageCompute(pstmt, params)

        val rs = pstmt.executeQuery()
        log.info("范围变量查询executeQuery")
        rs.next()

        var sqlDataType = rs.getMetaData.getColumnType(1)
        val isSigned = rs.getMetaData.isSigned(1)

        // MySQL has an unsigned integer which we need to allocate space for
        if (sqlDataType == Types.INTEGER && !isSigned) {
          sqlDataType = Types.BIGINT
        }

        val splitter = getSplitter(sqlDataType, mapNum, params, dbType)

        // 判断 最大,最小 是否为空,
        // 1. 如果有空,说明 null参与了最大,最小比较;
        // 2. 如果没有,需要求一下是否有为空的值,如果有,则增加一个partition
        val partitions: JList[SplitJdbcPartition] = splitter.splitPartitions(rs, splitCol)
        if(rs.getString(1) == null || rs.getString(2) == null) {
          // do nothing
        } else {
          //将数据库中为null的数据也给其分区 求 null值的个数
          val isNullNumPre=con.prepareStatement(DatabaseUtils.getCountValsQuery(dbType, splitCol, tableName, conditions, alias))
          pageCompute(isNullNumPre, params)
          val isNullNumPreRs=isNullNumPre.executeQuery()
          var isNullNum=0
          if(isNullNumPreRs.next()) {
            isNullNum = isNullNumPreRs.getInt(1)
          }
          if(isNullNum!=0){
            partitions.add(new SplitJdbcPartition(partitions.size(),splitCol+" is null",splitCol+" is null",params))
          }
        }

        partitions.toList.toArray
      }
    } catch {
      case e:Exception => {
        logError("获取分片异常", e)
        throw new RuntimeException(e)
      }
    } finally {
      if(con != null) {
        con.close()
      }
    }
  }


  override def compute(thePart: Partition, context: TaskContext) = new NextIterator[T] {

    //context.addOnCompleteCallback{ () => closeIfNeeded() }
    context.addTaskCompletionListener(tc => {
      closeIfNeeded()
    })
    val conn = getConnection()
    val part = thePart.asInstanceOf[SplitJdbcPartition]

    val partitionSql = if(StringUtils.isEmpty(conditions.trim)) {
      sql + " WHERE (" + part.lower +
        (if(StringUtils.isNotEmpty(part.upper)){ " AND " + part.upper} else {""})  +
        ")"
    } else {
      sql + " WHERE (" + conditions + ")" + " AND (" + part.lower +
        (if(StringUtils.isNotEmpty(part.upper)){ " AND " + part.upper} else {""}) +
        ")"
    }
    log.info("  execute sql === " + partitionSql)
    val stmt = conn.prepareStatement(partitionSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    // setFetchSize(Integer.MIN_VALUE) is a mysql driver specific way to force streaming results,
    // rather than pulling entire resultset into memory.
    // see http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-implementation-notes.html
    if (conn.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt.getFetchSize + " to force MySQL streaming ")
    }

    // 不再使用pageQuery分页
//    log.info(" page search ==== pagesize:" + part.pageSize + ",pageNo:" + part.pageNo)
    // 设置参数
    pageCompute(stmt, part.params)

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
  


  /**
   * @return the DBSplitter implementation to use to divide the table/query
   *         into InputSplits.
   */
  def getSplitter(sqlDataType: Int, numSplits: Int, params: JList[AnyRef], dbType: DataBaseType): DBSplitter = {

    if(sqlDataType == Types.NUMERIC
      || sqlDataType == Types.DECIMAL) {
      new BigDecimalSplitter(numSplits, params, dbType)
    } else if(sqlDataType == Types.BIT
      || sqlDataType == Types.BOOLEAN) {
      new BooleanSplitter(numSplits, params, dbType)
    } else if(sqlDataType == Types.INTEGER
      || sqlDataType == Types.TINYINT
      || sqlDataType == Types.SMALLINT
      || sqlDataType == Types.BIGINT) {
      new IntegerSplitter(numSplits, params, dbType)
    } else if(sqlDataType == Types.REAL
      || sqlDataType == Types.FLOAT
      || sqlDataType == Types.DOUBLE) {
      new FloatSplitter(numSplits, params, dbType)
    } else if(sqlDataType == Types.NVARCHAR
      || sqlDataType == Types.NCHAR) {
      new NTextSplitter(numSplits, params, dbType)
    } else if(sqlDataType == Types.CHAR
      || sqlDataType == Types.VARCHAR
      || sqlDataType == Types.LONGVARCHAR) {
      new TextSplitter(numSplits, params, dbType)
    } else if(sqlDataType == Types.DATE
      || sqlDataType == Types.TIME
      || sqlDataType == Types.TIMESTAMP) {
      new DateSplitter(numSplits, params, dbType)
    }  else {
      log.info("该数据类型[" + sqlDataType + "] 没有找到对应的splitter")
      null
    }

  }
}

object SplitJdbcRDD {
  def resultSetToObjectArray(rs: ResultSet) = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }
}
