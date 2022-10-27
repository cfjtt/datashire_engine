package com.eurlanda.datashire.engine.spark

import java.lang.{Integer => int}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException, Timestamp}
import java.util.logging.Logger

import com.eurlanda.datashire.engine.entity._
import com.eurlanda.datashire.engine.enumeration.{CDCActiveFlag, CDCSystemColumn}
import com.eurlanda.datashire.engine.util.datatypeConverter.TDataTypeConverter
import com.eurlanda.datashire.enumeration.{DataBaseType, NoSQLDataBaseType}
import com.eurlanda.datashire.server.utils.Constants
import com.mongodb.{BasicDBObject, MongoClient}
import org.apache.commons.lang
import org.apache.phoenix.compile.QueryPlan
import org.apache.phoenix.jdbc.PhoenixPreparedStatement

import scala.collection.{immutable, mutable}

/**
 * 数据源 工具类
 * Created by Juntao.Zhang on 2014/4/14.
 */
object DatabaseUtils extends Serializable {

    val log: Logger = Logger.getLogger("DatabaseUtils")

    def addNewRecord(dataSource: TDataSource,
                                    columns: List[TColumn],
                                    m: java.util.Map[Integer, DataCell],
                                    stmt: PreparedStatement) {
        var index = 1
        columns.foreach {
            value: TColumn =>
                val tmp = m.get(value.getId)
                TColumn.setValue(tmp, stmt, index, value)
                index += 1
        }
    }

    def updateSystemInNewMap(startDate: java.lang.Long, flag: String, version: Int,
                                            newm: java.util.Map[Integer, DataCell], columnMap: mutable.Map[Int, TColumn]) {
        columnMap.foreach(c => {
            val k = c._1
            val v = c._2
            v.getName match {
                case sd if CDCSystemColumn.DB_START_DATE_COL_NAME.getColValue == sd =>
                    if (newm.get(k) == null) {
                        newm.put(k, new DataCell(v.getData_type, new Timestamp(startDate)))
                    } else {
                        newm.get(k).setData(new Timestamp(startDate))
                    }
                case ed if CDCSystemColumn.DB_END_DATE_COL_NAME.getColValue == ed =>
                    if (newm.get(k) == null) {
                        newm.put(k, null)
                    } else {
                        newm.get(k).setData(null)
                    }
                case f if CDCSystemColumn.DB_FLAG_COL_NAME.getColValue == f =>
                    if (newm.get(k) == null) {
                        newm.put(k, new DataCell(v.getData_type, flag))
                    } else {
                        newm.get(k).setData(flag)
                    }
                case ver if CDCSystemColumn.DB_VERSION_COL_NAME.getColValue == ver =>
                    if (newm.get(k) == null) {
                        newm.put(k, new DataCell(v.getData_type, version))
                    } else {
                        newm.get(k).setData(version)
                    }
                case _ =>
                //do nothing
            }
        })
    }

    //获得 数据源 URL
    def getDataSourceUrl(dbi: TDataSource): String = {
        dbi.getType match {
            case DataBaseType.MYSQL =>
//                "jdbc:mysql://" + dbi.getHost + ":" + dbi.getPort + "/" + dbi.getDbName
//                "jdbc:mysql://" + dbi.getHost + "/" + dbi.getDbName
              // 针对大数据量读写时，mysql connection容易中断
            "jdbc:mysql://" + dbi.getHost + "/" + dbi.getDbName + "?autoReconnect=true&failOverReadOnly=false&maxReconnects=10"
            case DataBaseType.HANA =>
                "jdbc:sap://" + dbi.getHost + "?reconnect=true"
            case DataBaseType.SQLSERVER =>
//                "jdbc:sqlserver://" + dbi.getHost + ":" + dbi.getPort + ";DatabaseName=" + dbi.getDbName
                "jdbc:sqlserver://" + dbi.getHost + ";DatabaseName=" + dbi.getDbName
            case DataBaseType.HBASE_PHOENIX =>
//                "jdbc:phoenix:" + dbi.getHost + ":" + dbi.getPort
                "jdbc:phoenix:" + dbi.getHost
            case DataBaseType.ORACLE => {
                var tempStr:String = null
                if(dbi.getHost.split(";").length>=1) {
                    val strbf: StringBuffer = new StringBuffer
                    strbf.append("(DESCRIPTION =")
                    for (ss <- dbi.getHost.split(";")) {
                        val host: String = ss.split(":")(0)
                        val port: String = ss.split(":")(1)
                        strbf.append("(ADDRESS = (PROTOCOL = TCP)(HOST =" + host + ")(PORT = " + port + "))")
                    }
                    strbf.append("(LOAD_BALANCE=yes)(CONNECT_DATA=(SERVER = DEDICATED)(SERVICE_NAME = " + dbi.getDbName + ")))")
                    tempStr = strbf.toString
                    "jdbc:oracle:thin:@" + tempStr
                } else {
//                    "jdbc:oracle:thin:@" + dbi.getHost + ":" + dbi.getPort +":"+ dbi.getDbName
                    "jdbc:oracle:thin:@" + dbi.getHost + ":"+ dbi.getDbName
                }
                /**
                  * if(org.apache.commons.lang.StringUtils.isNotEmpty(tempStr)) {
                  * "jdbc:oracle:thin:@" + tempStr
                  * } else {
                  * "jdbc:oracle:thin:@" + dbi.getHost + ":" + dbi.getPort +":"+ dbi.getDbName
                  * }
                  */
            }
            case DataBaseType.DB2 =>
//                "jdbc:db2://" + dbi.getHost + ":" + dbi.getPort + "/" + dbi.getDbName
                "jdbc:db2://" + dbi.getHost + "/" + dbi.getDbName
            case DataBaseType.IMPALA =>
              // 暂时只做没有权限控制的, auth=noSasl
              "jdbc:impala://" + dbi.getHost + '/' + dbi.getDbName + ";auth=noSasl"
            case DataBaseType.TERADATA =>
              "jdbc:teradata://" + dbi.getHost + "/TMODE=TERA,CLIENT_CHARSET=EUC_CN,DATABASE=" + dbi.getDbName
            case _ =>
                throw new RuntimeException("没有匹配到对应的数据库，获得 数据源 URL" + dbi.getType);
        }
    }

    def totalSql(dbi: TDataSource): String = {
        val sql = "select count(1) from " + dbi.getSafeTableName + " " + dbi.getAlias + (if (isNotEmpty(dbi)) " where " + dbi.getFilter else "")
        sql
    }


    def isNotEmpty(dbi: TDataSource): Boolean = {
        dbi.getFilter != null && (!dbi.getFilter.trim.isEmpty)
    }

    def getKeyNames(pkColumns: scala.Array[TColumn]): scala.Array[String] = {
        val keyNames = new mutable.ArrayBuffer[String]
        pkColumns.foreach {
            value => if (value.isPrimaryKey) keyNames += value.getName
        }
        keyNames.toArray
    }

    /**
     * 对需要抽取的列，生成SQL
 *
     * @param columns
     * @return
     */
    def genSelectSql(columns: Array[TColumn], tableName:String,
                     filter:String, dataBaseType: DataBaseType, alias: String): String = {
        val sb = mutable.StringBuilder.newBuilder.append("SELECT ")
        var isFirst: Boolean = true
        for(column <- columns if column.isSourceColumn) {
            if(!isFirst) {
                sb.append(",").append(column.getName(dataBaseType, column.getName))
            } else {
                isFirst = false
                sb.append(column.getName(dataBaseType, column.getName))
            }
        }
        sb.append(" from ").append(tableName)

        dataBaseType match {
            case DataBaseType.DB2 =>
                sb.append(" as ")
            case _ => sb.append(" ")
        }
        sb.append(alias)

        if(lang.StringUtils.isNotEmpty(filter.trim)) {
            sb.append(" where ").append(filter)
        }
        sb.toString()
    }

    def getBoundingValsQuery(dbType: DataBaseType, splitCol: String, tableName: String, conditions: String, alias: String): String = {
        val sb = new StringBuilder().append("SELECT MIN(")
          .append(splitCol).append("), ").append("MAX(").append(splitCol)
          .append(") FROM ").append(tableName)

        dbType match {
            case DataBaseType.DB2 =>
                sb.append(" as ")
            case _ => sb.append(" ")
        }
        sb.append(alias)

        if(!org.apache.commons.lang.StringUtils.isEmpty(conditions.trim)) {
//            sb.append(" WHERE ( "+splitCol+" is not null and ").append(conditions).append(")")
            sb.append(" WHERE ( ").append(conditions).append(")")
        }else{
//            sb.append(" WHERE ( "+splitCol+" is not null ").append(")")
//            sb.append(" WHERE ( ").append(")")
        }
        sb.toString()
    }
    def getCountValsQuery(dbType: DataBaseType, splitCol: String, tableName: String, conditions: String, alias: String): String = {
        val sb = new StringBuilder().append("select count(*) as num from ").append(tableName)

        dbType match {
            case DataBaseType.DB2 =>
                sb.append(" as ")
            case _ => sb.append(" ")
        }
        sb.append(alias)

        if(!org.apache.commons.lang.StringUtils.isEmpty(conditions.trim)) {
            sb.append(" WHERE ( "+splitCol+" is null and ").append(conditions).append(")")
        }else{
            sb.append(" WHERE ( "+splitCol+" is null ").append(")")
        }
        sb.toString()
    }

    /**
     * 从JDBC 中取出来的数据   ResultSet -> JMap[Integer, DataCell]
 *
     * @param columns
     * @param dataTypeConverter
     * @return
     */
    def mapRow(columns: Array[TColumn], dataTypeConverter: TDataTypeConverter): (ResultSet) => java.util.Map[Integer, DataCell] = {
        (rs: ResultSet) => {
            val rsMap: java.util.Map[int, DataCell] = new java.util.HashMap[int, DataCell]()
            // 获取所有要获取的 columns
            for (column: TColumn <- columns) {
                val dc: DataCell =
                    if (!column.isSourceColumn) {
                        if (Constants.DEFAULT_EXTRACT_COLUMN_NAME.equalsIgnoreCase(column.getName)) {
                            new DataCell(TDataType.TIMESTAMP, column.getExtractTime)
                        } else {
                            throw new RuntimeException("不存在非sourceColumen:" + column.getName)
                        }
                    } else {
                        // 判断是否存在 jdbc类型不为空
                        if(column.getJdbcDataType != null) {
                            TColumn.getValue(dataTypeConverter, column, rs)
                        } else {
                            TColumn.getValue(column, rs)
                        }
                    }
                rsMap.put(column.getId, dc)
            }
//            logDebug("get from db data :" + rsMap)
            rsMap
        }
    }

    def getQueryPlan(connection: Connection, selectSql: String, params: java.util.List[Object]): QueryPlan = {
//        val queryPlan = connection.createStatement().unwrap(classOf[PhoenixStatement]).optimizeQuery(selectSql);
        val pstmt = connection.prepareStatement(selectSql)
        var idx = 1
        if(params != null) {
            for(param <- params.toArray) {
                pstmt.setObject(idx, param)
                idx += 1
            }
        }

        val queryPlan = pstmt.unwrap(classOf[PhoenixPreparedStatement]).optimizeQuery()
//        val queryPlan = connection.createStatement().unwrap(classOf[PhoenixStatement]).compileQuery(selectSql);
        queryPlan.iterator()
        queryPlan
    }

    def pageSql(dbi: TDataSource, pageSize: Int, orderColumn: String): String = {
        dbi.getType match {
            case DataBaseType.MYSQL =>
                val sql = "select * from " + dbi.getSafeTableName + " " + dbi.getAlias + (if (isNotEmpty(dbi)) " where " + dbi.getFilter else "") + " limit ?, ?"
//                println("MySql pageSql : " + sql)
                sql
            case DataBaseType.SQLSERVER=>
                if(lang.StringUtils.isEmpty(orderColumn)) {
                    throw new RuntimeException("sql server 分页抽取必须有排序字段")
                }
                val sql = "select top " + pageSize + " * from (SELECT ROW_NUMBER() OVER (ORDER BY " + orderColumn + ") AS rn, * FROM " +
                        dbi.getSafeTableName + " " + dbi.getAlias + (if (isNotEmpty(dbi)) " where " + dbi.getFilter else "") +
                        ") a where rn > ?" + " ORDER BY " + orderColumn
//                println("sql server pageSql : " + sql)
                sql
            case DataBaseType.TERADATA=>
                if(lang.StringUtils.isEmpty(orderColumn)) {
                    throw new RuntimeException("teradata 分页抽取必须有排序字段")
                }
                val sql = "select top " + pageSize + " * from (SELECT ROW_NUMBER() OVER (ORDER BY " + orderColumn + ") AS rn, "+orderColumn+" FROM " +
                  dbi.getSafeTableName + " " + dbi.getAlias + (if (isNotEmpty(dbi)) " where " + dbi.getFilter else "") +
                  ") a where rn > ?" + " ORDER BY " + orderColumn
                //                println("sql server pageSql : " + sql)
                sql
            /**  hbase 的已经不需要了
              * case DataBaseType.HBASE_PHOENIX =>
              * var keys = ""
              * var start = 0
              * val size = columns.length
              * var keys2 = ""
              * val columnsStr = getKeyNames(columns).map("\"" + _ + "\"").mkString(",")
              * columns.foreach(value => {
              * if (value.isPrimaryKey) {
              * if (start==0) {
              * keys += " ( "
              * }
              * keys += value.getName(DataBaseType.HBASE_PHOENIX)
              * keys2 += "?"
              * start += 1
              * if(start<size) {
              * keys += ","
              * keys2 += ","
              * }
              * }
              * })
              * keys += ") >= (" + keys2 + ") "

              * val sql = "select * from " + dbi.getSafeTableName + " " + dbi.getAlias + " WHERE " + keys + (if (isNotEmpty(dbi)) " and " + dbi.getFilter else "") + " order by " + columnsStr + " limit ?"
              * //                println("hbase pageSql : " + sql)
              * sql
              */
            case DataBaseType.ORACLE =>
                val sql = "select * from (select a.*, ROWNUM rn from(select * from " +
                        dbi.getSafeTableName + " " + dbi.getAlias +" where ROWNUM<=? " + (if (isNotEmpty(dbi)) " and " +
                        dbi.getFilter else "") +  " ) a ) where rn>?"
//                println("oracle pageSql : " + sql)
                sql
            case DataBaseType.DB2 =>
                val sql = "select a.* from (SELECT ROW_NUMBER() OVER (ORDER BY " + orderColumn + ") AS rn, b.* FROM " +
                        dbi.getSafeTableName  + " as " + dbi.getAlias +
//                  " as b " +
                  (if (isNotEmpty(dbi)) " where " + dbi.getFilter else "") +
                        ") a where rn > ?"  + " and rn<=? "
//                println("db2 pageSql : " + sql)
                sql
            case DataBaseType.HANA =>
                val sql = "select * from " + dbi.getSafeTableName + " " + dbi.getAlias + (if (isNotEmpty(dbi)) " where " + dbi.getFilter else "") + " limit ? offset ?"
                //                println("MySql pageSql : " + sql)
                sql
            case _ =>
                throw new RuntimeException("没有匹配到对应的数据库，暂时不支持该数据库的分页sql,=" + dbi.getType);
        }
    }

    def dropTable(dataSource: TDataSource): Unit = {
        try {
            System.out.println("drop data fall table " + dataSource.getTableName + " ....")
            val conn: Connection = DatabaseUtils.getConnection(dataSource)
            conn.setAutoCommit(false)

            dataSource.getType match {
                case DataBaseType.SQLSERVER =>
                    conn.createStatement.executeUpdate("IF EXISTS (SELECT * FROM sys.objects WHERE name='" + dataSource.getSafeTableName + "')  DROP TABLE " + dataSource.getSafeTableName)
                case _ =>
                    conn.createStatement.executeUpdate("drop table if exists " + dataSource.getSafeTableName)
            }
            conn.commit()
            conn.close()
        }
        catch {
            case e: SQLException =>
                e.printStackTrace()
        }
    }

    def truncateNoSql(dataSource: TNoSQLDataSource): Unit = {
        dataSource.getType match {
            case NoSQLDataBaseType.MONGODB =>
                new MongoClient(dataSource.getHost)
                  .getDB(dataSource.getDbName)
                  .getCollection(dataSource.getTableName)
                  .remove(new BasicDBObject());
            case _ =>
                throw new RuntimeException("不存在该NOSQL数据库类型" + dataSource.getType)
        }
    }

    def truncate(dataSource: TDataSource, columnSet: immutable.Set[TColumn]): Unit = {
        dataSource.getType match {
            case DataBaseType.HBASE_PHOENIX =>
              /**
                * val configuration = HBaseConfiguration.create()
                * val hosts = dataSource.getHost.split(":")
                * assert(hosts.length == 2)
                * configuration.set("hbase.zookeeper.quorum", hosts(0))
                * configuration.set("hbase.zookeeper.property.clientPort", hosts(1))
                * val admin = new HBaseAdmin(configuration)
                * val tableName = dataSource.getSafeTableName

                * val tableDescriptor: HTableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName.toUpperCase))
                * admin.disableTable(TableName.valueOf(tableName.toUpperCase))
                * admin.deleteTable(TableName.valueOf(tableName.toUpperCase))
                * admin.createTable(tableDescriptor)
                */
              // 由于phoenix 对于删除表时会产生脏数据,现在先慢慢删除

                val conn: Connection = getConnection(dataSource)
                conn.setAutoCommit(false)
                conn.prepareStatement("delete from " + dataSource.getSafeTableName).executeUpdate()
                conn.commit()
                if (conn != null && !conn.isClosed) {
                    conn.close()
                }

//                logInfo("清空hbase表成功 " + dataSource.getDbName + "." + dataSource.getTableName)
            case DataBaseType.DB2 =>
                val conn: Connection = getConnection(dataSource)
                conn.setAutoCommit(false)
                conn.prepareStatement("alter table " + dataSource.getSafeTableName + " activate not logged initially with empty table ").executeUpdate()
                conn.commit()
                if (conn != null && !conn.isClosed) {
                    conn.close()
                }
//                logInfo("清空数据库表成功 " + dataSource.getDbName + "." + dataSource.getTableName)
            case _ =>
                val conn: Connection = getConnection(dataSource)
                conn.setAutoCommit(false)
                conn.prepareStatement("truncate table " + dataSource.getSafeTableName).executeUpdate()
                conn.commit()
                if (conn != null && !conn.isClosed) {
                    conn.close()
                }
//                logInfo("清空数据库表成功 " + dataSource.getDbName + "." + dataSource.getTableName)
        }
    }

    // 注册驱动
    def getConnection(dbi: TDataSource): Connection = {
        dbi.getType match {
            // mysql 数据库
            case DataBaseType.MYSQL =>
                classOf[com.mysql.jdbc.Driver]
                DriverManager.getConnection(getDataSourceUrl(dbi), dbi.getUserName, dbi.getPassword)
            // sql server 数据库
            case DataBaseType.SQLSERVER =>
                classOf[com.microsoft.sqlserver.jdbc.SQLServerDriver]
                DriverManager.getConnection(getDataSourceUrl(dbi), dbi.getUserName, dbi.getPassword)
            case DataBaseType.ORACLE =>
                classOf[oracle.jdbc.driver.OracleDriver]
                Class.forName("oracle.jdbc.driver.OracleDriver")
                // 设置环境的参数  这个参数应该和服务器端一致,这样才能保证数据正确
                DriverManager.getConnection(getDataSourceUrl(dbi), dbi.getUserName, dbi.getPassword)
            case DataBaseType.HBASE_PHOENIX =>
                val st = System.currentTimeMillis()
                try {
                    classOf[org.apache.phoenix.jdbc.PhoenixDriver]
                    val conn = DriverManager.getConnection(getDataSourceUrl(dbi))
                    conn
                } finally {
//                    logInfo("connection cost time :" + (System.currentTimeMillis() - st))
                }
            case DataBaseType.DB2 =>
                classOf[com.ibm.db2.jcc.DB2Driver]
                Class.forName("com.ibm.db2.jcc.DB2Driver")
                DriverManager.getConnection(getDataSourceUrl(dbi), dbi.getUserName, dbi.getPassword)
            case DataBaseType.HANA =>
                classOf[com.sap.db.jdbc.Driver]
//                Class.forName("com.sap.db.jdbc.Driver")
                DriverManager.getConnection(getDataSourceUrl(dbi), dbi.getUserName, dbi.getPassword)
            case DataBaseType.IMPALA =>
                classOf[com.cloudera.impala.jdbc41.Driver]
                DriverManager.getConnection(getDataSourceUrl(dbi), dbi.getUserName, dbi.getPassword)
            case DataBaseType.TERADATA =>
                classOf[com.teradata.jdbc.TeraDriver]
                DriverManager.getConnection(getDataSourceUrl(dbi), dbi.getUserName, dbi.getPassword)
            // 没有匹配的，抛出异常
            case _ => throw new RuntimeException("没有匹配到对应的数据库，请联系系统管理员修改程序")
        }
    }

  def updateCDC1SourceColumns(dataSource: TDataSource, conn: Connection,
                                               columns: List[TColumn],
                                               busKeyColumns: List[TColumn], priKeyColumns: List[TColumn],
                                               columnMap: mutable.Map[Int, TColumn],
                                               scd1Cols: mutable.Map[Integer, DataCell],
                                               newm: java.util.Map[Integer, DataCell], oldm: Map[Integer, DataCell]) = {
        val sql = if(dataSource.getType == DataBaseType.HBASE_PHOENIX) {
            //UPSERT INTO T.TEST_1(id1,id2,val1,val2) SELECT id1,id2,?,? FROM T.TEST_1 where bus1=? and bus2=?;
            var first = true
            var busKeySql = ""
            var names1Sql = ""
            var names2Sql = ""
            busKeyColumns.foreach(v => {
                if (!first) busKeySql += " and "
                busKeySql += v.getName(dataSource.getType) + "=?"
                first = false
            })
            first = true
            priKeyColumns.foreach(v => {
                if (!first) {
                    names1Sql += ","
                    names2Sql += ","
                }
                names1Sql += v.getName(dataSource.getType)
                names2Sql += v.getName(dataSource.getType)
                first = false
            })
            scd1Cols.keySet.seq.foreach(key => {
                if (!first) {
                    names1Sql += ","
                    names2Sql += ","
                }
                names1Sql += columnMap.get(key).get.getName(dataSource.getType)
                names2Sql += "?"
                first = false
            })
            "UPSERT INTO " + dataSource.getSafeTableName + " (" + names1Sql + ") SELECT " + names2Sql + " FROM " + dataSource.getSafeTableName + " where " + busKeySql
        } else {
            //update table set val1=?,val2=? where bus1=? and bus2=?
            var first = true
            var busKeySql = ""
            var updateSql = ""
            busKeyColumns.foreach(v => {
                if (!first) busKeySql += " and "
                busKeySql += v.getName(dataSource.getType) + "=?"
                first = false
            })
            first = true
            scd1Cols.keySet.seq.foreach(key => {
                if (!first) updateSql += ","
                updateSql += columnMap.get(key).get.getName(dataSource.getType) + "=?"
                first = false
            })
            "update " + dataSource.getSafeTableName + " set " + updateSql + " where (" + busKeySql + ")"
        }
        val busKeyDCs = mutable.ArrayBuffer.empty[DataCell]
        busKeyColumns.foreach(v => {
            busKeyDCs += oldm.get(v.getId).get
        })
        val stat = conn.prepareStatement(sql)
        var index = 1
        scd1Cols.keySet.seq.foreach(key => {
            TColumn.setValue(scd1Cols.get(key).get, stat, index, columnMap(key))
            index += 1
        })
        busKeyColumns.foreach(col => {
            TColumn.setValue(oldm.get(col.getId).get, stat, index, col)
            index += 1
        })
        stat.executeUpdate()
        stat.close()
    }

  def updateSystemColumns(dataSource: TDataSource, conn: Connection, priKeyColumns: List[TColumn],
                                           currentTime: Long, oldm: Map[Integer, DataCell]) = {
        var tmp = ""

        val sql = if(dataSource.getType == DataBaseType.HBASE_PHOENIX) {
            var params = "?,?"
            priKeyColumns.foreach(v => {
                tmp += "," + v.getName(dataSource.getType)
                params += ",?"
            })
            "UPSERT INTO " + dataSource.getSafeTableName + " (" + CDCSystemColumn.DB_END_DATE_COL_NAME.getColValue + "," + CDCSystemColumn.DB_FLAG_COL_NAME.getColValue + tmp + ") VALUES (" + params + ")"
        } else {
            var first = true
            priKeyColumns.foreach(v => {
                if (!first) tmp += " and "
                tmp += v.getName(dataSource.getType) + "=?"
                first = false
            })
            "update " + dataSource.getSafeTableName + " set " + CDCSystemColumn.DB_END_DATE_COL_NAME.getColValue + "=?," + CDCSystemColumn.DB_FLAG_COL_NAME.getColValue + "=? where (" + tmp + ")"
        }

        val stat = conn.prepareStatement(sql)
        stat.setTimestamp(1, new Timestamp(currentTime))
        stat.setString(2, CDCActiveFlag.IN_ACTIVE.getValue)
        var index = 3
        priKeyColumns.foreach(v => {
            TColumn.setValue(oldm.get(v.getId).get, stat, index, v)
            index += 1
        })
        stat.executeUpdate()
        stat.close()
    }

    def getByKeySQL(dataSource: TDataSource, busKeyColumns: List[TColumn]): String = {
        var tmp = ""
        var first = 0
        dataSource.getType match {
            // mysql 数据库
            case DataBaseType.MYSQL | DataBaseType.HBASE_PHOENIX =>
                busKeyColumns.foreach(v => {
                    if (first != 0) {
                        tmp += " and "
                    }
                    tmp +=  v.getName(dataSource.getType) + "=?"
                    first += 1
                })
                "select * from " + dataSource.getSafeTableName + " where (" + tmp + " and " + CDCSystemColumn.DB_FLAG_COL_NAME.getColValue + "='" + CDCActiveFlag.ACTIVE.getValue + "'" + ") limit 1"
            // sql server 数据库
            case DataBaseType.SQLSERVER =>
                busKeyColumns.foreach(v => {
                    if (first != 0) {
                        tmp += " and "
                    }
                    tmp += v.getName(dataSource.getType) + "=?"
                    first += 1
                })
                "select top 1 * from " + dataSource.getSafeTableName + " where (" + tmp + " and " + CDCSystemColumn.DB_FLAG_COL_NAME.getColValue + "='" + CDCActiveFlag.ACTIVE.getValue + "'" + ")"
            case DataBaseType.ORACLE =>
                busKeyColumns.foreach(v => {
                    if (first != 0) {
                        tmp += " and "
                    }
                    tmp += v.getName(dataSource.getType) + "=?"
                    first += 1
                })
                "select * from " + dataSource.getSafeTableName + " where (" + tmp + " and " + CDCSystemColumn.DB_FLAG_COL_NAME.getColValue + "='" + CDCActiveFlag.ACTIVE.getValue + "'" + ") and rownum<2"
            case _ => throw new RuntimeException("没有匹配到对应的数据库，请联系系统管理员修改程序")
        }
    }

    def genInsertSQL(dataSource: TDataSource, columnSet: Set[TColumn]): String = {
        var values = ""
        var names = ""
        var first = 0
        columnSet.toSeq.sortBy(_.getId).foreach {
            value: TColumn =>
                if (first != 0) {
                    names += ","
                    values += ","
                }
                names += value.getName(dataSource.getType)
                /**
                  * if(value.isGuid) {
                  * //                    values += "NEXT VALUE FOR ds_sys_sequence." + dataSource.getTableName + "_id_sequence"
                  * values += "'" + UUIDUtil.genUUID() + "'";
                  * } else {
                  * values += "?"
                  * }
                  */
                values += "?"
                first += 1
        }

        val sql = if(dataSource.getType == DataBaseType.HBASE_PHOENIX) {
            "UPSERT INTO " + dataSource.getSafeTableName + "(" + names + ") values(" + values + ")"
        } else {
            "insert into " + dataSource.getSafeTableName + "(" + names + ") values(" + values + ")"
        }

        sql
    }

    def genSelectSql(columns:Array[TColumn], dbType: DataBaseType, tableName:String, alias: String):String = {
        val sb = new StringBuilder().append("select ")

        var isFirst = true
        for(column <- columns) {
            if(Constants.DEFAULT_EXTRACT_COLUMN_NAME == column.getName && !column.isSourceColumn) {
                // 抽取时间列，为系统添加的列
            } else {
                if(isFirst) {
                    isFirst = false;
                } else {
                    sb.append(",")
                }
                sb.append(column.getName(dbType))
            }
        }
        sb.append(" from ").append(tableName)
        dbType match {
            case DataBaseType.DB2 =>
                sb.append(" as ")
            case _ => sb.append(" ")
        }
        sb.append(alias)
        sb.toString()
    }

}