package com.eurlanda.datashire.engine.spark.translation

import com.eurlanda.datashire.engine.entity.{TColumn, TDataType}
import com.eurlanda.datashire.engine.spark.stream.{SKafkaSquid, SSquid}
import com.eurlanda.datashire.engine.util.ConstantUtil
import com.eurlanda.datashire.entity._
import com.eurlanda.datashire.enumeration.TransformationTypeEnum
import com.eurlanda.datashire.server.utils.Constants
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

/**
  * Created by zhudebin on 16/1/21.
  */
class KafkaExtractBuilder(context: StreamBuilderContext, squid: Squid) extends SquidBuilder {

  val currentSquid = squid.asInstanceOf[KafkaExtractSquid]

  val columns = buildColumns()

  override def translate(): List[SSquid] = {
    val sKafkaSquid = buildExtract()

    if(currentSquid.isIs_persisted) {
      val sDataFallSquid = buildDataFall()
      sDataFallSquid.preSquids=List(sKafkaSquid)
      List(sKafkaSquid, sDataFallSquid)
    } else {
      List(sKafkaSquid)
    }
  }

  def buildExtract():SKafkaSquid = {

    val preSquid = context.getPrevSquids(currentSquid).get(0)

    assert(preSquid != null, currentSquid + " 不存在前置squid")

    val kafkaSquid = preSquid.asInstanceOf[KafkaSquid]

    new SKafkaSquid(kafkaSquid.getZkQuorum,
      currentSquid.getGroup_id,
      Map(getExtractSquidTableName(currentSquid.getSource_table_id) -> currentSquid.getNumPartitions),
      StorageLevel.MEMORY_AND_DISK_2, columns.toList)
  }

  def buildDataFall():SSquid = {
    val list = StreamBuilderUtil.doTranslateDataFall(context, this, currentSquid)
    if(list != null && list.size == 1) {
      list(0)
    } else {
      null
    }

  }

  def getExtractSquidTableName(source_table_id: Int): String = {
    if (source_table_id == 0) {
      throw new RuntimeException("未找到extractSquid[" + squid.getId + "]引用的sourceTable[" + source_table_id + "]")
    }
    val sql: String = "select table_name from ds_source_table where id=" + source_table_id
    val ret: java.util.Map[String, AnyRef] = ConstantUtil.getJdbcTemplate.queryForMap(sql)
    return ret.get("table_name").asInstanceOf[String]
  }

  def buildColumns()  = {
    currentSquid.getColumns.map(c => {
      val tColumn = new TColumn()
      tColumn.setId(c.getId)
      // 判断是否有transformationLink连接,有连线的将Tcolumn的名字变为referenceColumn的名字

      // 1. 获取 column上的 虚拟transformation
      val tran = currentSquid.getTransformations.filter(t => {
        t.getTranstype == TransformationTypeEnum.VIRTUAL.value() && t.getColumn_id == c.getId
      }).get(0)
      // 如果:存在 虚拟trans的column_id == column.id, 并且有transformationLink连上,
      //  则需要将TColumn.setName()为 referenceColumn的名字
      // 否则: TColumn.setName()为column的名字

      // 2. 获取连接到该transformation 的link,有连线size大于0,否则等于0
      val transformationLinks = currentSquid.getTransformationLinks.filter(tl => tl.getTo_transformation_id == tran.getId)

      if(transformationLinks.size > 0) {  // 有连线
        val t = buildTransMap().getOrElse(transformationLinks.get(0).getFrom_transformation_id, null)
        if(t != null) {
          currentSquid.getSourceColumns.map(sc => {
            if(sc.getColumn_id == t.getColumn_id) { // 匹配 referenceColumn
              tColumn.setName(sc.getName)                         // 将 referenceColumn的名字赋给TColumn
              tColumn.setData_type(TDataType.STRING)              // 默认就是varchar,所以写死了
            }
          })
        }
      } else {
        tColumn.setName(null)
      }

      // 3. 没有连线的名字为空
      // 如果没有连线,有如下几种情况: extract_date
      if(tColumn.getName == null) {
        if(Constants.DEFAULT_EXTRACT_COLUMN_NAME.equalsIgnoreCase(c.getName)) {
          tColumn.setName(Constants.DEFAULT_EXTRACT_COLUMN_NAME.toUpperCase)
          tColumn.setData_type(TDataType.TIMESTAMP)
          tColumn.setSourceColumn(false)
        } else {
          throw new RuntimeException("不存在不需要连线的系统列:" + c.getName)
        }
      }

      tColumn
    })
  }

  def buildTransMap()  = {
    currentSquid.getTransformations.map(t => {
      (t.getId, t)
    }).toMap
  }
}
