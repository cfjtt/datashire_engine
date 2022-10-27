package org.apache.spark

import java.lang.{Integer => int}
import java.sql._
import java.util.{List => JList, Map => JMap}
import javax.xml.stream.XMLEventReader
import javax.xml.stream.events.XMLEvent

import com.eurlanda.datashire.engine.entity._
import com.eurlanda.datashire.engine.enumeration.{CDCActiveFlag, CDCSystemColumn, RemoteType}
import com.eurlanda.datashire.engine.spark.DatabaseUtils._
import com.eurlanda.datashire.engine.spark.rdd._
import com.eurlanda.datashire.engine.spark._
import com.eurlanda.datashire.engine.util._
import com.eurlanda.datashire.engine.util.datatypeConverter._
import com.eurlanda.datashire.engine.{util => _}
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype
import com.eurlanda.datashire.enumeration.{DataBaseType, EncodingType, FileType}
import com.eurlanda.datashire.server.utils.Constants
import com.mongodb.BasicDBObject
import com.mongodb.hadoop.util.MongoConfigUtil
import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.bson.BSONObject

import scala.Array
import scala.collection.mutable

/**
 * Created by zhudebin on 13-12-13.
 */
class CustomJavaSparkContext(val scc: SparkContext) extends JavaSparkContext(scc) with Logging {

  def this(conf: SparkConf) = this(new SparkContext(conf))

    private val jarSet: java.util.Set[String] = new java.util.HashSet[String]()

    override def addJar(path: String) {
        if (jarSet.contains(path)) {
            // do nothing
        } else {
            super.addJar(path)
            jarSet.add(path)
        }
    }

    def this(master: String, appName: String) {
        this(new SparkContext(master, appName))
    }

    def this(master: String, appName: String, sparkHome: String, jarFile: String) = {
        this(new SparkContext(master, appName, sparkHome, Seq(jarFile)))
    }

    def mongoRDD(dbi: TNoSQLDataSource, allColumns: Array[TColumn], topN:Integer): JavaRDD[java.util.Map[java.lang.Integer, DataCell]] = {
        mongoRDD("mongodb://" + dbi.getHost + "/" + dbi.getDbName + "." + dbi.getTableName,
            dbi.getFilter, allColumns)
    }

    /**
     * 对MongoDB进行抽取
      *
      * @param inputURI
     * @param inputQuery
     * @return
     */
    def mongoRDD(inputURI:String, inputQuery:String, allColumns: Array[TColumn]): JavaRDD[java.util.Map[java.lang.Integer, DataCell]] = {
        // Set configuration options for the MongoDB Hadoop Connector.
        val mongodbConfig:Configuration = new Configuration();
        // MongoInputFormat allows us to read from a live MongoDB instance.
        // We could also use BSONFileInputFormat to read BSON snapshots.
        mongodbConfig.set(MongoConfigUtil.JOB_INPUT_FORMAT,
            "com.mongodb.hadoop.MongoInputFormat");
        // MongoDB connection string naming a collection to use.
        // If using BSON, use "mapred.input.dir" to configure the directory
        // where BSON files are located instead.
        mongodbConfig.set(MongoConfigUtil.INPUT_URI, inputURI)
//            "mongodb://192.168.137.4:27017/test.test")

        //    mongodbConfig.set(MongoConfigUtil.INPUT_QUERY,"{i : {$gt : 3},\"$where\":\"function(){return this.i + this.i <100}\"}")
        mongodbConfig.set(MongoConfigUtil.INPUT_QUERY, inputQuery)
//            "{\"type\":\"database\"}")

//        mongodbConfig.set(MongoConfigUtil.INPUT_FIELDS, inputFields.map(s => "\"" + s + "\":1").mkString("{",",","}"))
        mongodbConfig.set(MongoConfigUtil.INPUT_FIELDS, allColumns.filter(c => StringUtils.isNotEmpty(c.getName)).map(c => "\"" + c.getName + "\":1").mkString("{",",","}"))
//            "{\"_id\":1, \"type\":1, \"info.x\":1}")

        // Create an RDD backed by the MongoDB collection.
        val documents:RDD[(Object, BSONObject)]  = sc.newAPIHadoopRDD(
            mongodbConfig,            // Configuration
            classOf[MongoInputFormat],   // InputFormat: read from a live cluster.
            classOf[Object],             // Key class
            classOf[BSONObject]          // Value class
        )

        // 对应的映射关系
        documents.map(t2 => {
            val jmap:java.util.Map[java.lang.Integer, DataCell] = new java.util.HashMap[java.lang.Integer, DataCell]()
            for(column <- allColumns) {
                if("_id" == column.getName) {
                    val value = t2._1.toString
                    jmap.put(column.getId, new DataCell(column.getData_type, value))
                } else if(Constants.DEFAULT_EXTRACT_COLUMN_NAME == column.getName && !column.isSourceColumn) {
                    jmap.put(column.getId, new DataCell(column.getData_type, column.getExtractTime.clone()));
                } else {
                    val value = BSONUtil.getByKey(t2._2.asInstanceOf[BasicDBObject], column.getName.split("\\."), 0)
                    if(value == null) {
                      jmap.put(column.getId, new DataCell(column.getData_type, value))
                    } else {
                      jmap.put(column.getId, new DataCell(column.getData_type, value.toString))
                    }
                }
            }
            jmap
        })
    }

    /**
     * phoenix hbase 分页抽取
      *
      * @param dbi
     * @param allColumns
     * @param topN 该属性暂时没有起作用 todo
     * @return
     */
    def phoenixJdbcRDD(dbi: TDataSource, allColumns: Array[TColumn], topN:Integer): JavaRDD[java.util.Map[java.lang.Integer, DataCell]] = {
        val columns = allColumns.filter(p => p.getId>0)
        val selectSql = genSelectSql(columns,dbi.getTableName, dbi.getFilter, dbi.getType, dbi.getAlias)
        phoenixJdbcRDD(dbi, selectSql, mapRow(columns, getDataTypeConverter(dbi.getType)), columns, dbi.getParams)
    }

    def jdbcRDD(dbi: TDataSource, allColumns: Array[TColumn], topN:Integer): JavaRDD[java.util.Map[java.lang.Integer, DataCell]] = {
        import com.eurlanda.datashire.engine.spark.DatabaseUtils.{genSelectSql, getConnection, getDataSourceUrl, mapRow, pageSql, totalSql}

        var columns:Array[TColumn] = allColumns
        // 判断是否为 hbase
        if(dbi.getType == DataBaseType.HBASE_PHOENIX) {
            columns = allColumns.filter(p => p.getId>0)
        }

        if(dbi.getType == DataBaseType.HBASE_PHOENIX) {
            val selectSql = genSelectSql(columns,dbi.getTableName, dbi.getFilter, dbi.getType, dbi.getAlias)
//            logInfo("select sql : " + selectSql)
            phoenixJdbcRDD(dbi, selectSql, mapRow(columns, getDataTypeConverter(dbi.getType)), columns, dbi.getParams)
        } else {
            this.jdbcRDD(
                getDataSourceUrl(dbi),
                dbi.getUserName,
                dbi.getPassword,
                dbi.getTableName,
                dbi.getType,
                totalSql(dbi),
                pageSql(dbi, ConfigurationUtil.getSqlPageSize, getOrderColumn(columns, dbi.getType)),
                columns,
//                pkColumns,
                ConfigurationUtil.getSqlPageSize,
                () => {
                    getConnection(dbi)
                },
                dbi.getFilter,
                if(topN == null) 0l else topN.toLong,
                dbi.getParams
            )
        }

    }

    def splitJdbcRDD(dbi: TDataSource, splitCol: String, splitNum: Int, allColumns: Array[TColumn]): JavaRDD[java.util.Map[java.lang.Integer, DataCell]] = {
        import com.eurlanda.datashire.engine.spark.DatabaseUtils.getConnection
        splitJdbcRDD(dbi.getSafeTableName,
            dbi.getAlias,
            dbi.getType,
            allColumns,
            splitCol,
            splitNum,
            () => {
                getConnection(dbi)
            },
            dbi.getFilter,
            dbi.getParams
        )
    }

    def splitJdbcRDD(tableName: String,
                     alias: String,
                     dataBaseType: DataBaseType,
                     columns: Array[TColumn],
                     splitCol:String,
                     mapNum:Int,
                     getConnection: () => Connection,
                     filter: String,
                     params:java.util.List[Object]): JavaRDD[java.util.Map[java.lang.Integer, DataCell]] = {
        // 数据类型转换器
        val dataTypeConverter: TDataTypeConverter = getDataTypeConverter(dataBaseType)
        val pageCompute = (pstmt:PreparedStatement,part:JList[AnyRef]) => {
            if(part != null && part.size()>0) {
                for(i <- 0 until part.size()) {
                    pstmt.setObject(i+1, part.get(i))
                }
            }
        }
        import com.eurlanda.datashire.engine.spark.DatabaseUtils.{genSelectSql, mapRow}
        val splitColType = if(columns.filter(t => t.getName == splitCol).length == 0) {
          DbBaseDatatype.UNKNOWN
        } else {
          columns.filter(t => t.getName == splitCol)(0).getDbBaseDatatype
        }
        new SplitJdbcRDD[JMap[java.lang.Integer, DataCell]](scc, getConnection,
            genSelectSql(columns, dataBaseType, tableName, alias),
            dataBaseType, tableName, alias,
            new TColumn().getName(dataBaseType, splitCol),
            splitColType,
            filter, mapNum,
            pageCompute, mapRow( columns, dataTypeConverter), params)
    }

    private def getOrderColumn(columns:Array[TColumn], dbType:DataBaseType)= {
        // 第一步，先查找主键，如果有主键，则使用主键
        val pkColumns =  columns.filter(c => {
            c.isPrimaryKey
        })
        val oc = new StringBuilder()
        if(pkColumns.size>0) {
            pkColumns.foreach(c => {
                oc.append(",").append(c.getName(dbType))
            })
        } else {
            val cs = columns.filter(c => {
                c.isSourceColumn &&
//                !c.getName.equalsIgnoreCase("extraction_date") &&
                c.getDbBaseDatatype() != DbBaseDatatype.XML &&
                c.getDbBaseDatatype() != DbBaseDatatype.GEOGRAPHY &&
                c.getDbBaseDatatype() != DbBaseDatatype.TEXT &&
                c.getDbBaseDatatype() != DbBaseDatatype.NTEXT &&
                c.getDbBaseDatatype() != DbBaseDatatype.IMAGE
            })
            cs.foreach(c => {
                oc.append(",").append(c.getName(dbType))
            })
        }
        val str = oc.toString()
        if (str.length>0) {
            str.substring(1)
        } else {
            null
        }
    }

    /**
     * @param url
     * @param dataBaseType
     * @param totalSql
     * @param pageSql
     * @param pageSize
     * @param columns
     * @return
     */
    def jdbcRDD(url: String, username: String, password: String, tableName: String,
                dataBaseType: DataBaseType, totalSql: String,
                pageSql: String,
                columns: Array[TColumn],
                pageSize: Long = 5000,
                getConnection: () => Connection,
                filter: String,
                topN:Long,
                params:java.util.List[Object]): JavaRDD[java.util.Map[java.lang.Integer, DataCell]] = {
        // 数据类型转换器
        val dataTypeConverter: TDataTypeConverter = getDataTypeConverter(dataBaseType)

        logInfo(" -- database extract page sql -- " + pageSql)

        // 此处不需要关闭连接
        var total: Long = 0

        def getPageNos: Int = {
            val conn: Connection = getConnection()
            // 获取总数
            val pst = conn.prepareStatement(totalSql)
            if(params != null && params.size()>0) {
                var idx = 1
                for(i <- params.toArray) {
                    pst.setObject(idx, i)
                    idx += 1
                }
            }
            val rs: ResultSet = pst.executeQuery()
            rs.next()
            total = rs.getLong(1)
            if (conn != null && !conn.isClosed) {
                conn.close()
            }
            if(topN>0 && total>topN) {
                total = topN
            }
            if (total % pageSize == 0) total./(pageSize).toInt else total./(pageSize).+(1).toInt
        }

        val pageCompute = (stmt: PreparedStatement, partition: CustomJdbcPartition) => {
            // 通过判断数据库类型确定分页SQL
            var index = 1
            dataBaseType match {
                //  limit (pageNo-1)*pageSize, pageSize
                case DataBaseType.MYSQL =>
                    if(partition.params!=null && partition.params.size>0) {
                        for(obj <- partition.params.toArray) {
                            stmt.setObject(index, obj)
                            index += 1
                        }
                    }
                    //logInfo("pageNo" + partition.pageNo + ", pagesize=" + partition.pageSize)
                    stmt.setLong(index, (partition.pageNo - 1) * partition.pageSize)
                    index += 1
                    stmt.setLong(index, partition.pageSize)
                // RowNumber > (pageNo-1)*pageSize and RowNumber< pageSize
                case DataBaseType.SQLSERVER | DataBaseType.TERADATA=>
                    //                    stmt.setLong(1,partition.pageSize)
//                    stmt.setLong(index, (partition.pageNo - 1) * partition.pageSize)
//                    index += 1
                    if(partition.params!=null && partition.params.size>0) {
                        for(obj <- partition.params.toArray) {
                            stmt.setObject(index, obj)
                            index += 1
                        }
                    }
                    stmt.setLong(index, (partition.pageNo - 1) * partition.pageSize)
                case DataBaseType.ORACLE =>
                    stmt.setLong(index, (partition.pageNo) * partition.pageSize);
                    index += 1
                    if(partition.params!=null && partition.params.size>0) {
                        for(obj <- partition.params.toArray) {
                            stmt.setObject(index, obj)
                            index += 1
                        }
                    }
                    stmt.setLong(index, (partition.pageNo - 1) * partition.pageSize);
                case DataBaseType.DB2 =>
                    if(partition.params!=null && partition.params.size>0) {
                        for(obj <- partition.params.toArray) {
                            stmt.setObject(index, obj)
                            index += 1
                        }
                    }
                    stmt.setLong(index, (partition.pageNo-1) * partition.pageSize)
                    index += 1
                    stmt.setLong(index, (partition.pageNo) * partition.pageSize)
                case DataBaseType.HANA =>
                    // limit pageSize offset startNo
                    if(partition.params!=null && partition.params.size>0) {
                        for(obj <- partition.params.toArray) {
                            stmt.setObject(index, obj)
                            index += 1
                        }
                    }
                    stmt.setLong(index, partition.pageSize)
                    index += 1
                    stmt.setLong(index, (partition.pageNo - 1) * partition.pageSize)
                case _ => throw new RuntimeException("没有匹配到对应的数据库，请联系系统管理员修改程序")
            }
        }
        import com.eurlanda.datashire.engine.spark.DatabaseUtils.mapRow
        new CustomJdbcRDD[JMap[java.lang.Integer, DataCell]](scc,
            getConnection, pageSql, getPageNos, pageSize,
            pageCompute,
            mapRow( columns, dataTypeConverter),params
        ).toJavaRDD()
    }



    /**
     * 根据不同的数据库类型，返回相应地数据类型转换器
      *
      * @param dataBaseType
     * @return
     */
    private def getDataTypeConverter(dataBaseType: DataBaseType): TDataTypeConverter = {
        // 数据类型转换器
        dataBaseType match {
            case DataBaseType.MYSQL =>
                new MysqlDataTypeConverter()
            case DataBaseType.SQLSERVER =>
                new SQLSERVERDataTypeConverter()
            case DataBaseType.ORACLE =>
                new OracleDataTypeConverter()
            case DataBaseType.HBASE_PHOENIX =>
                new HbaseDataTypeConverter()
            case _ =>
                new CommonDataTypeConverter()
        }
    }

    /**
     * phoenix queryPlan 获取数据查询
      *
      * @param selectSql
     * @param mapRow
     * @param columns
     * @return
     */
    def phoenixJdbcRDD(dbi: TDataSource,
                       selectSql: String,
                       mapRow: (ResultSet) => JMap[Integer, DataCell],
                       columns: Array[TColumn],
                       params: java.util.List[Object]): JavaRDD[JMap[Integer, DataCell]] = {
        // todo 验证此处为啥是2，里面是8
        new PhoenixExtractRDD[JMap[Integer, DataCell]](scc, dbi, selectSql, mapRow, params)
    }

    /**
     * 从FTP服务器/共享文件夹抽取数据，转换
      *
      * @param ip
     * @param port
     * @param files
     * @param username
     * @param password
     * @param fileType
     * @param encoding
     * @param fileRecordSeparator
     * @return
     */
    def ftpSharedFolderFile(ip: String,
                            port: Int,
                            ftpConInfo: FtpUtil.FtpConnectionInfo,
                            files: Array[String],
                            username: String,
                            password: String,
                            fileType: String,
                            remoteType: RemoteType,
                            //                      firstDataRowNo: Int,
                            serverEncoding: EncodingType, // ftp 服务器编码
                            encoding: EncodingType,
                            fileRecordSeparator: FileRecordSeparator): JavaRDD[java.util.Map[java.lang.Integer, DataCell]] = {
        val fileTypeEnum = FileType.valueOf(fileType.toUpperCase)
        //xml
        if (fileTypeEnum == FileType.XML) {
            new FtpSharedFolderXmlRDD[java.util.Map[java.lang.Integer, DataCell]](this, ip, port,
              ftpConInfo,
              files, username, password, fileTypeEnum, remoteType, encoding,
                (xmlEvent: XMLEvent, reader: XMLEventReader) => {
                    fileRecordSeparator.separate(xmlEvent, reader)
                }).toJavaRDD()
        }
        //pdf txt word(doc,docx)
        else if (fileTypeEnum == FileType.PDF || fileTypeEnum == FileType.TXT || FileType.isWord(fileTypeEnum)) {
            new FtpSharedFolderTxtWordPdfRDD[java.util.Map[java.lang.Integer, DataCell]](this,
                ip, port,
                ftpConInfo,
                files, username, password, fileTypeEnum,
                remoteType,
                fileRecordSeparator.getFirstDataRowNo,
                serverEncoding,
                encoding,
                (line: Any) => {
                    line match {
                        case s: String =>
                            fileRecordSeparator.separate(s)
                        case _ =>
                            new java.util.HashMap[Integer, DataCell]
                    }
                },
                fileRecordSeparator.getPosition,
                fileRecordSeparator.getRowDelimiter
            ).toJavaRDD()
        }
        // log(common,extended,iis,combined) excel(xls,xlsx,csv)
        else {
            if (FileType.isExcel(fileTypeEnum)) {
                new ExcelSharedFolderRDD(this, ip, port,
                  ftpConInfo,
                  files, username, password, fileTypeEnum, remoteType,
                  fileRecordSeparator.getFirstDataRowNo, serverEncoding, encoding,
                    (line: Any) => {
                        line match {
                            case s: java.util.List[String] =>
                                fileRecordSeparator.separate(s)
                            case _ =>
                                new java.util.HashMap[Integer, DataCell]
                        }
                    }).toJavaRDD()
            } else {
                new LogFtpSharedFolderRDD(this, ip, port,
                  ftpConInfo,
                  files, username, password, fileTypeEnum, remoteType,
                  fileRecordSeparator.getFirstDataRowNo, serverEncoding, encoding,
                    (line: Any) => {
                        line match {
                            case s: String =>
                                fileRecordSeparator.separate(s)
                            case _ =>
                                new java.util.HashMap[Integer, DataCell]
                        }
                    }).toJavaRDD()
            }
        }
    }

    //hdfs excel(xls,xlsx)
    def hdfsExcelFile(hdfsUrl: String, paths: Array[String], encoding: EncodingType, fileRecordSeparator: FileRecordSeparator): JavaRDD[java.util.Map[Integer, DataCell]] = {
        val rdd = new HdfsExcelRDD[java.util.Map[java.lang.Integer, DataCell]](
            this,
            hdfsUrl,
            paths,
            FileType.valueOf(fileRecordSeparator.getFileType.toUpperCase),
            fileRecordSeparator.getFirstDataRowNo,
            encoding,
            (line: Any) => {
                line match {
                    case s: java.util.List[String] =>
                        fileRecordSeparator.separate(s)
                    case _ =>
                        new java.util.HashMap[Integer, DataCell]
                }
            })
        rdd.toJavaRDD()
    }

    //hdfs word pdf
    def hdfsWordPdfFile(hdfsUrl: String,paths: Array[String], encoding: EncodingType, fileLineSeparator: FileRecordSeparator): JavaRDD[java.util.Map[Integer, DataCell]] = {
        val rdd = new HdfsWordPdfRDD[java.util.Map[java.lang.Integer, DataCell]](
            this,
            hdfsUrl,
            paths,
            FileType.valueOf(fileLineSeparator.getFileType.toUpperCase),
            fileLineSeparator.getFirstDataRowNo,
            encoding,
            (line: Any) => {
                line match {
                    case s: String =>
                        fileLineSeparator.separate(s)
                    case _ =>
                        new java.util.HashMap[Integer, DataCell]
                }
            },
            fileLineSeparator.getPosition,
            fileLineSeparator.getRowDelimiter)
        rdd.toJavaRDD()
    }

    def fallDataToMongo(dataSource: TNoSQLDataSource, columnSet: java.util.Set[TColumn], rdd: JavaRDD[java.util.Map[Integer, DataCell]], truncateExistingData: Boolean) {
        if(truncateExistingData) {
            import com.eurlanda.datashire.engine.spark.DatabaseUtils.truncateNoSql
            logInfo("truncate table " + dataSource.getTableName)
            truncateNoSql(dataSource)
        }
        // 获取 _id字段
        val columns = columnSet.toArray(new Array[TColumn](0))
        val pks = columns.filter(t => t.getName == "_id" && t.isPrimaryKey)
        val values = columns.filter(t => !(t.getName == "_id" && t.isPrimaryKey))
        val mRDD:RDD[(Object, BSONObject)] = if(pks != null && pks.length==1) {
            val pk = pks(0)
            rdd.rdd.map(m => {
                val bson = new BasicDBObject()
                for(c <- values) {
                    bson.put(c.getName, DataCellUtil.getMongoData(m.get(c.getId)))
                }
                (DataCellUtil.getMongoData(m.get(pk.getId)), bson)
            })
        } else {
            rdd.rdd.map(m => {
                val bson = new BasicDBObject()
                for(c <- values) {
                    bson.put(c.getName, DataCellUtil.getMongoData(m.get(c.getId)))
                }
                (null, bson)
            })
        }
        // Create a separate Configuration for saving data back to MongoDB.
        val outputConfig:Configuration = new Configuration();
        outputConfig.set(MongoConfigUtil.JOB_OUTPUT_FORMAT,
            "com.mongodb.hadoop.MongoOutputFormat");
        outputConfig.set(MongoConfigUtil.OUTPUT_URI,
            "mongodb://" + dataSource.getHost + "/"
              + dataSource.getDbName + "."
              + dataSource.getTableName
//              + "?connectTimeoutMS=200000")
              + "?connectTimeoutMS=20000&waitQueueMultiple=100&waitQueueTimeoutMS=20000")
        mRDD.saveAsNewAPIHadoopFile(
            "file:///this-is-completely-unused",
            classOf[Object],
            classOf[BSONObject],
            classOf[MongoOutputFormat[Object,BSONObject]],
            outputConfig
        )
    }

    // 数据落地
    def fallDataRDD(dataSource: TDataSource, columnSet: java.util.Set[TColumn],
                    rdd: JavaRDD[java.util.Map[Integer, DataCell]], truncateExistingData: Boolean)  {
        import com.eurlanda.datashire.engine.spark.DatabaseUtils.{genInsertSQL, truncate}
//        import scala.collection.JavaConverters.asScalaSetConverter
        import scala.collection.JavaConversions._
        val _columnSet = columnSet.toSet
        //检查表结构合法性 delete 前台做这个事
//        checkDataSource(dataSource, _columnSet)
        val tableName=dataSource.getTableName
        if (truncateExistingData) {
            logInfo("truncate table " + tableName )
            truncate(dataSource, _columnSet)
        }
        /*
        //表在服务端创建   表若不存在，则创建
        val conn = getConnection(dataSource)
        val isexiststable= ModelMysqlUtil.isExistsTable(conn, tableName)
        if(! isexiststable) {
           mysqlCreatPredictTable(conn,tableName,_columnSet)
        }*/
        val insertSql: String = genInsertSQL(dataSource, _columnSet)
        logInfo("insert sql:" + insertSql)
        if (dataSource.isCDC) {
            fallDataRDDWithCDC(dataSource, columnSet, rdd, truncateExistingData, insertSql)
        } else {
            fallDataRDDWithoutCDC(dataSource, columnSet, rdd, truncateExistingData, insertSql)
        }
    }

  /**
    * @param con
    * @param tableName
    * @param columnSet
    */
  private def  mysqlCreatPredictTable( con :Connection,  tableName :String, columnSet:Set[TColumn]) {
      val sb = new StringBuilder();
      sb.append( "create table if not exists " + tableName + " (" )
      for(tcolumn<- columnSet.slice(0,columnSet.size-1)){
          sb.append(tcolumn.getName+" " + tcolumn.getData_type+",")
      }
      sb.append(columnSet.last.getName+" " + columnSet.last.getData_type)
      sb.append(")")
    con.createStatement().execute(sb.toString);
  }

  /**
   * CDC落地
    *
    * @param dataSource
   * @param columnSet
   * @param rdd
   * @param truncateExistingData
   * @return
   */
  def fallDataRDDWithCDC(dataSource: TDataSource,
                         columnSet: java.util.Set[TColumn],
                         rdd: JavaRDD[java.util.Map[Integer, DataCell]],
                         truncateExistingData: Boolean,
                         insertSql: String) = {
      import com.eurlanda.datashire.engine.spark.DatabaseUtils.{getByKeySQL, updateCDC1SourceColumns, updateSystemColumns}

  //import scala.collection.JavaConverters.asScalaSetConverter
      import scala.collection.JavaConversions._
      val _columns = columnSet.toSeq.sortBy(_.getId).toList
      val busKeyColumns = new mutable.ListBuffer[TColumn]
      val priKeyColumns = new mutable.ListBuffer[TColumn]
      val columnMap = mutable.Map[Int, TColumn]()
      _columns.foreach(v => {
          if (v.isBusinessKey) {
              busKeyColumns += v
          }
          if (v.isPrimaryKey) {
              priKeyColumns += v
          }
          columnMap += (v.getId -> v)
      })
      if (busKeyColumns.isEmpty) {
          throw new RuntimeException("business key columns is empty!")
      }
      if (priKeyColumns.isEmpty) {
          throw new RuntimeException("primary key columns is empty!")
      }
      val selectSql = getByKeySQL(dataSource, busKeyColumns.toList)
      val logger = log
      logInfo("select sql:" + selectSql)
      sc.runJob(rdd, (context: TaskContext, iter: Iterator[java.util.Map[Integer, DataCell]]) => {
          val st = System.currentTimeMillis()
          import com.eurlanda.datashire.engine.spark.DatabaseUtils.{addNewRecord, getConnection, updateSystemInNewMap}
          val conn: Connection = getConnection(dataSource)
          val insertStmt: PreparedStatement = conn.prepareStatement(insertSql)
          val selectStmt: PreparedStatement = conn.prepareStatement(selectSql)
          conn.setAutoCommit(false)
          logger.info("===============================")
          logger.info("fall data column:" + _columns)
          logger.info("fall data begin :")
          for (m <- iter) {
              if (m != null && m.size() != 0) {
  //                    logger.debug("fall data iter data : " + m)
                  try {
                      val scd1Cols = mutable.Map[Integer, DataCell]()
                      val scd2Cols = mutable.Map[Integer, DataCell]()
                      val oldm = mutable.Map[Integer, DataCell]()
                      var hasHistoryRecord = false
                      var hasChangeRecord = false
                      var isNewRecord = true
                      var version = 1
                      var index = 1
                      busKeyColumns.foreach(col => {
                          TColumn.setValue(m.get(col.getId), selectStmt, index, col)
                          index += 1
                      })
                      val rs = selectStmt.executeQuery()
                      if (rs.next()) {
                          hasHistoryRecord = true
                          _columns.foreach(v => {
                              val oldVal = TColumn.getValue(v, rs).getData
                              oldm += (Integer.valueOf(v.getId) -> new DataCell(v.getData_type, oldVal))
                              //get version
                              if (v.getName == CDCSystemColumn.DB_VERSION_COL_NAME.getColValue) {
                                  version = oldVal.toString.toInt
                              }
                              if (v.isNotCDCSystemColumn && !v.isPrimaryKey) {
                                  val newVal = m.get(v.getId).getData
                                  if (newVal != oldVal) {
                                      hasChangeRecord = true
                                      if (v.getCdc == 1) {
                                          scd1Cols += (Integer.valueOf(v.getId) -> new DataCell(v.getData_type, newVal))
                                      } else if (v.getCdc == 2) {
                                          scd2Cols += (Integer.valueOf(v.getId) -> new DataCell(v.getData_type, newVal))
                                      }
                                  }
                              }
                          })
                      }
                      val currentTime = System.currentTimeMillis()
                      //判断CDC为2的是否有变更
                      if (hasHistoryRecord) {
                          if (hasChangeRecord) {
                              //判断CDC为1的是否有变更
                              if (!scd1Cols.isEmpty) {
                                  //cdc 判断CDC为1的是否有变更,update该字段的值
                                  updateCDC1SourceColumns(dataSource, conn, _columns, busKeyColumns.toList, priKeyColumns.toList,
                                      columnMap, scd1Cols, m, oldm.toMap)
                                  isNewRecord = false
                              }
                              if (!scd2Cols.isEmpty) {
                                  //修改上一条记录的end_date,flag
                                  updateSystemColumns(dataSource, conn, priKeyColumns.toList, currentTime - 1000, oldm.toMap)
                                  //修改 start_date(current time),end_date(null),flag(T), version(version + 1)
                                  updateSystemInNewMap(currentTime, CDCActiveFlag.ACTIVE.getValue, version + 1, m, columnMap)
                                  //有则添加一条记录
                                  isNewRecord = true
                              }
                          } else {
                              //非主键属性 无改变
                              isNewRecord = false
                          }
                      } else {
                          isNewRecord = true
                          //修改 start_date(current time),end_date(null),flag(T), version(1)
                          updateSystemInNewMap(currentTime, CDCActiveFlag.ACTIVE.getValue, version, m, columnMap)
                      }
                      if (isNewRecord) {
                          addNewRecord(dataSource, _columns, m, insertStmt)
                          insertStmt.executeUpdate()
                      }
                      conn.commit()
                      rs.close()
                  } catch {
                      case e: Exception =>
                          logger.error("Transaction is being rolled back")
                          conn.rollback()
                          throw new RuntimeException("iter set value :" + e)
                  }
                  logger.debug("fall data iter end")
              }
          }
          if (conn != null && !conn.isClosed) {
              conn.close()
          }
          logger.info("fall data end, cost time :" + (System.currentTimeMillis() - st))
          logger.info("================================")
      })
  }

  /**
   * 非CDC落地
    *
    * @param dataSource
   * @param columnSet
   * @param rdd
   * @param truncateExistingData
   * @return
   */
  def fallDataRDDWithoutCDC(dataSource: TDataSource, columnSet: java.util.Set[TColumn],
                            rdd: JavaRDD[java.util.Map[Integer, DataCell]], truncateExistingData: Boolean, sql: String) = {
//        import scala.collection.JavaConverters.asScalaSetConverter
      import scala.collection.JavaConversions._
      val _columnSet = columnSet.toSet
      val logger = log
      sc.runJob(rdd, (context: TaskContext, iter: Iterator[java.util.Map[Integer, DataCell]]) => {
          val st = System.currentTimeMillis()
          import com.eurlanda.datashire.engine.spark.DatabaseUtils.getConnection
//            val conn: Connection = getConnection(dataSource)
//            conn.setAutoCommit(false)
//            val stmt: PreparedStatement = conn.prepareStatement(sql)
          logger.info("===============================")
          logger.info("fall data begin,sql:" + sql)

          var count = 0
          var conn: Connection = null
          if(iter != null && !iter.isEmpty) {
            try {
              conn = getConnection(dataSource)
              conn.setAutoCommit(false)
              val stmt: PreparedStatement = conn.prepareStatement(sql)
              for (m <- iter) {
                  if (m != null && m.size() != 0) {
                      var index = 1
                      _columnSet.toSeq.sortBy(_.getId).foreach {
                          value: TColumn =>
                              val tmp =
                                  if (!value.isSourceColumn && value.isGuid) {
                                      new DataCell(TDataType.STRING, UUIDUtil.genUUID)
                                  } else {
                                      m.get(value.getId)
                                  }
                              TColumn.setValue(tmp, stmt, index, value)
                              index += 1
                      }
                      count += 1
                      stmt.addBatch()
                      //分页提交
                      if (count % 2000 == 0) {
                          stmt.executeBatch()
                          conn.commit()
                          count = 0
                      }
                  }
              }
              if (count > 0) {
                  stmt.executeBatch()
                  conn.commit()
              }
            } catch {
              case e: Exception =>
                conn.rollback()
                conn.commit()
                if (conn != null && !conn.isClosed) {
                  conn.close()
                }
                //throw new RuntimeException("iter set value 异常:", e)
              throw new RuntimeException("iter set value 异常:"+e.getMessage)
            } finally {
              if (conn != null && !conn.isClosed) {
                conn.close()
              }
            }
          }
          logger.info("fall data end, cost time :" + (System.currentTimeMillis() - st))
          logger.info("================================")
      })
  }

  /**
    * def saveAsHadoopDataset[K, V](self: JavaRDD[(K, V)], conf: JobConf):Unit = self.rdd.withScope {
    * // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    * val hadoopConf = conf
    * val wrappedConf = new SerializableConfiguration(hadoopConf)
    * val outputFormatInstance = hadoopConf.getOutputFormat
    * val keyClass = hadoopConf.getOutputKeyClass
    * val valueClass = hadoopConf.getOutputValueClass
    * if (outputFormatInstance == null) {
    * throw new SparkException("Output format class not set")
    * }
    * if (keyClass == null) {
    * throw new SparkException("Output key class not set")
    * }
    * if (valueClass == null) {
    * throw new SparkException("Output value class not set")
    * }
    * SparkHadoopUtil.get.addCredentials(hadoopConf)

    * logDebug("Saving as hadoop file of type (" + keyClass.getSimpleName + ", " +
    * valueClass.getSimpleName + ")")

    * // 验证 是否检验输出文件夹是否存在
    * val isOutputSpecValidationEnabled = false
    * if (isOutputSpecValidationEnabled) {
    * // FileOutputFormat ignores the filesystem parameter
    * val ignoredFs = FileSystem.get(hadoopConf)
    * hadoopConf.getOutputFormat.checkOutputSpecs(ignoredFs, hadoopConf)
    * }

    * val writer = new CustomSparkHadoopWriter(hadoopConf)
    * writer.preSetup()

    * val writeToFile = (context: TaskContext, iter: Iterator[(K, V)]) => {
    * val config = wrappedConf.value
    * // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
    * // around by taking a mod. We expect that no task will be attempted 2 billion times.
    * val taskAttemptId = (context.taskAttemptId % Int.MaxValue).toInt

    * val (outputMetrics, bytesWrittenCallback) = initHadoopOutputMetrics(context)

    * writer.setup(context.stageId, context.partitionId, taskAttemptId)
    * writer.open()
    * var recordsWritten = 0L

    * Utils.tryWithSafeFinally {
    * while (iter.hasNext) {
    * val record = iter.next()
    * writer.write(record._1.asInstanceOf[AnyRef], record._2.asInstanceOf[AnyRef])

    * // Update bytes written metric every few records
    * maybeUpdateOutputMetrics(bytesWrittenCallback, outputMetrics, recordsWritten)
    * recordsWritten += 1
    * }
    * } {
    * writer.close()
    * }
    * writer.commit()
    * bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
    * outputMetrics.setRecordsWritten(recordsWritten)
    * }

    * self.rdd.context.runJob(self, writeToFile)
    * writer.commitJob()
    * }

    * private def initHadoopOutputMetrics(context: TaskContext): (OutputMetrics, Option[() => Long]) = {
    * val bytesWrittenCallback = SparkHadoopUtil.get.getFSBytesWrittenOnThreadCallback()
    * val outputMetrics = new OutputMetrics(DataWriteMethod.Hadoop)
    * if (bytesWrittenCallback.isDefined) {
    * context.taskMetrics.outputMetrics = Some(outputMetrics)
    * }
    * (outputMetrics, bytesWrittenCallback)
    * }

    * private def maybeUpdateOutputMetrics(bytesWrittenCallback: Option[() => Long],
    * outputMetrics: OutputMetrics, recordsWritten: Long): Unit = {
    * if (recordsWritten % PairRDDFunctions.RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES == 0) {
    * bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
    * outputMetrics.setRecordsWritten(recordsWritten)
    * }
    * }
    */
}
