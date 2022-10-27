package com.eurlanda.datashire.engine.spark.translation

import java.util

import com.eurlanda.datashire.engine.entity.{TColumn, TDataType, TNoSQLDataSource}
import com.eurlanda.datashire.engine.spark.stream.{SDataFallSquid, SDestMongoSquid, SSquid}
import com.eurlanda.datashire.engine.translation.TranslateUtil
import com.eurlanda.datashire.engine.translation.TranslateUtil.DataFallInfo
import com.eurlanda.datashire.entity.{DataSquid, DbSquid, NOSQLConnectionSquid}
import com.eurlanda.datashire.enumeration.{NoSQLDataBaseType, SquidTypeEnum}

/**
  * Created by zhudebin on 16/8/8.
  */
object StreamBuilderUtil {

  def doTranslateDataFall(context: StreamBuilderContext,
                          squidBuilder: SquidBuilder,
                          currentSquid: DataSquid):List[SSquid] = {

    val dbSquid = if(currentSquid.getDestination_squid_id ==0) {
      null
    } else {
      context.getSquidById(currentSquid.getDestination_squid_id)
    }

    //    require(dbSquid == null, "stage squid 指定的落地对象不能为空")
    if(dbSquid == null) {
      return List[SSquid]()
    } else if(SquidTypeEnum.parse(dbSquid.getSquid_type) == SquidTypeEnum.DBSOURCE
        || SquidTypeEnum.parse(dbSquid.getSquid_type) == SquidTypeEnum.CLOUDDB
    || SquidTypeEnum.parse(dbSquid.getSquid_type)==SquidTypeEnum.TRAININGDBSQUID) {
      val dataFallInfo:DataFallInfo = TranslateUtil.translateDataFallInfo(currentSquid, dbSquid.asInstanceOf[DbSquid])
      //    print(dataFallInfo.gettDataSource())
      if(dataFallInfo != null) {
        val sdf = new SDataFallSquid(dataFallInfo.gettDataSource(), dataFallInfo.gettColumnMap())
        sdf.preSquids = List(squidBuilder.getCurrentLastSSquid(context))
        List(sdf)
      } else {
        null
      }
    } else if(SquidTypeEnum.parse(dbSquid.getSquid_type) == SquidTypeEnum.MONGODB) {
      val nosqlConnectionSquid: NOSQLConnectionSquid = dbSquid.asInstanceOf[NOSQLConnectionSquid]
      val dataBaseType: NoSQLDataBaseType = NoSQLDataBaseType.parse(nosqlConnectionSquid.getDb_type)
      dataBaseType match {
        case NoSQLDataBaseType.MONGODB =>
          val tDataSource: TNoSQLDataSource = new TNoSQLDataSource
          tDataSource.setCDC(false)
          tDataSource.setDbName(nosqlConnectionSquid.getDb_name)
          tDataSource.setUserName(nosqlConnectionSquid.getUser_name)
          tDataSource.setPassword(nosqlConnectionSquid.getPassword)
          tDataSource.setType(dataBaseType)
          tDataSource.setTableName(currentSquid.getTable_name)
          tDataSource.setHost(nosqlConnectionSquid.getHost)
          tDataSource.setPort(nosqlConnectionSquid.getPort)

          val tColumnMap: java.util.Set[TColumn] = new util.HashSet[TColumn]
          //			List<Column> columns = stageSquid.getColumns();
          import scala.collection.JavaConversions._
          for (c <- currentSquid.getColumns) {
            val tColumn: TColumn = new TColumn
            tColumn.setId(c.getId)
            tColumn.setName(c.getName)
            tColumn.setLength(c.getLength)
            tColumn.setPrecision(c.getPrecision)
            tColumn.setPrimaryKey(c.isIsPK)
            tColumn.setData_type(TDataType.sysType2TDataType(c.getData_type))
            tColumn.setNullable(c.isNullable)
            tColumn.setCdc(if (c.getCdc == 1) 2 else 1)
            tColumn.setBusinessKey(c.getIs_Business_Key == 1)
            tColumnMap.add(tColumn)
          }

          val sMongoSquid = new SDestMongoSquid(tDataSource, tColumnMap, currentSquid.isTruncate_existing_data_flag == 1)
          sMongoSquid.preSquids = List(squidBuilder.getCurrentLastSSquid(context))
          List(sMongoSquid)
        case _ =>
          throw new RuntimeException("不能处理该NOSQL数据库类型" + dataBaseType)
      }
    } else {
      null
    }
  }

}
