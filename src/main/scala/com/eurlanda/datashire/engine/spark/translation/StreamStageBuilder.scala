package com.eurlanda.datashire.engine.spark.translation

import java.util
import java.util.{Collections, Comparator, List => JList, Map => JMap}

import com.eurlanda.datashire.engine.entity._
import com.eurlanda.datashire.engine.spark.Logging
import com.eurlanda.datashire.engine.spark.stream._
import com.eurlanda.datashire.engine.translation.{StreamStageTransformationActionTranslator, TranslateUtil}
import com.eurlanda.datashire.entity._
import com.eurlanda.datashire.enumeration.JoinType
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer

/**
  * Created by zhudebin on 16/3/15.
  */
class StreamStageBuilder(context: StreamBuilderContext, squid: Squid) extends SquidBuilder with Logging {
  override val currentSquid: StreamStageSquid = squid.asInstanceOf[StreamStageSquid]

  override def translate(): List[ESquid] = {
//    val preSquid = context.getSquidOut(context.getPrevSquids())

    val joinSquids = doTranslateJoin(currentSquid)
    appendToSquids(currentBuildedSquids, joinSquids)

    val filterSquids = doTranslateSquidFilter(currentSquid)
    appendToSquids(currentBuildedSquids, filterSquids)

    val transformationSquids = doTranslateTransformations(currentSquid)
    appendToSquids(currentBuildedSquids, transformationSquids)

    val aggregateSquids = doTranslateAggregate(currentSquid)
    appendToSquids(currentBuildedSquids, aggregateSquids)

    val windowSquids = doTranslateWindow(currentSquid)
    appendToSquids(currentBuildedSquids, windowSquids)

    val dataFallSquids = doTranslateDataFall(currentSquid)
    appendToSquids(currentBuildedSquids, dataFallSquids)

    currentBuildedSquids.toList
  }

  def appendToSquids(currentBuildedSquids: ListBuffer[ESquid], eSquids:List[ESquid]) {
    if(eSquids != null && eSquids.size > 0) {
      currentBuildedSquids.appendAll(eSquids)
    }
  }

  /**
    * 将 StreamStageSquid 翻译为 SJoinSquid 或者 SUnionSquid
    *
    * @param currentSquid
    * @return
    */
  def doTranslateJoin(currentSquid: StreamStageSquid):List[ESquid] = {
    var ret: List[ESquid] = null
    // 判断是否有 join 部分
    if (currentSquid == null
      || currentSquid.getJoins == null
      || currentSquid.getJoins.size < 2) {
      logDebug(currentSquid.getSquidflow_id + "_" + currentSquid.getId + " 不存在 join")
      return null
    } else {
      val squidJoins: JList[SquidJoin] = currentSquid.getJoins
      // 先排序
      Collections.sort(squidJoins, new Comparator[SquidJoin]() {
        def compare(o1: SquidJoin, o2: SquidJoin): Int = {
          return o1.getPrior_join_id - o2.getPrior_join_id
        }
      })
      // 根据join类型,翻译为 join或者union  -- 现有功能为要么全是Join, 要么全部是 uion
      if (squidJoins.get(1).getJoinType > JoinType.BaseTable.value
        && squidJoins.get(1).getJoinType < JoinType.Unoin.value) {
        ret = translateJoinSquid2SJoinSquid(squidJoins, currentSquid)
      } else {
        ret = translateUnionSquid(squidJoins)
      }
    }
    ret

  }

  /**
    * StreamStageSquid 将filter 部分翻译为对应的 SFilterSquid
    *
    * @param currentSquid
    * @return
    */
  def doTranslateSquidFilter(currentSquid: StreamStageSquid):List[SSquid] = {
    val filter = currentSquid.getFilter.trim
    if(StringUtils.isNotEmpty(filter)) {
      import scala.collection.JavaConversions._
      val variableMap:JMap[String, DSVariable] = context.variable
      val name2squidMap:JMap[String, Squid] = context.name2squid
      val sfe = TranslateUtil.translateTFilterExpression(currentSquid, variableMap, name2squidMap)
      if(sfe != null) {
        val sfq = new SFilterSquid(sfe)
        sfq.preSquids = List(getCurrentLastSSquid(context))
        return List(sfq)
      }
    }
    null
  }

  /**
    * 将流式stage squid 的transformation 部分翻译
    *
    * @param currentSquid
    * @return
    */
  def doTranslateTransformations(currentSquid: StreamStageSquid):List[SSquid] = {
    val jobContext = context.genTJobContext()
    jobContext.setSquidId(currentSquid.getId)
    jobContext.setSquidName(currentSquid.getName)

    val sTransformationSquid:STransformationSquid = new STransformationSquid(isPreviousExceptionSquid = false,
      jobContext = jobContext)
    sTransformationSquid.squidId = currentSquid.getId
    sTransformationSquid.tTransformationActions = new StreamStageTransformationActionTranslator(currentSquid,
      context.idKeyGenerator, context.variable, context, sTransformationSquid).translateTTransformationActions().asInstanceOf[util.ArrayList[TTransformationAction]]

    sTransformationSquid.preSquids = List(getCurrentLastSSquid(context))

    List(sTransformationSquid)
  }

  def doTranslateAggregate(currentSquid: StreamStageSquid):List[SSquid] = {
    val aggregateInfo = TranslateUtil.translateAggregateInfo(currentSquid)
    if(aggregateInfo != null) {
      val aggregateSquid = new SAggregateSquid(aggregateInfo.getGroupKeyList, aggregateInfo.getAaList)
      aggregateSquid.preSquids = List(getCurrentLastSSquid(context))
      return List(aggregateSquid)
    }
    null
  }

  def doTranslateWindow(currentSquid: StreamStageSquid):List[SSquid] = {

    if(currentSquid.getENABLE_WINDOW) {
      val windowSquid = new SWindowSquid(currentSquid.getWINDOW_DURATION)
      windowSquid.preSquids = List(getCurrentLastSSquid(context))
      List(windowSquid)
    } else {
      null
    }
  }

  def doTranslateDataFall(currentSquid: StreamStageSquid):List[SSquid] = {
    if(currentSquid.isIs_persisted && currentSquid.getDestination_squid_id > 0) {
      StreamBuilderUtil.doTranslateDataFall(context, this, currentSquid)
    } else {
      List[SSquid]()
    }
  }

  def translateJoinSquid2SJoinSquid(squidJoins: JList[SquidJoin], currentSquid: StreamStageSquid):List[ESquid] = {
    val preSSquids: JList[ESquid] = new util.ArrayList[ESquid]
    val joinTypes: util.List[JoinType] = new util.ArrayList[JoinType]
    val joinExpressions: util.List[String] = new util.ArrayList[String]
    val squidNames: util.List[String] = new util.ArrayList[String]
    val id2Columns: JList[JMap[Integer, TStructField]] = new util.ArrayList[util.Map[Integer, TStructField]]

    import scala.collection.JavaConversions._
    for(squidJoin <- squidJoins.toList) {
      val squid = context.getSquidById(squidJoin.getJoined_squid_id)
      preSSquids.add(context.getSquidOut(squidJoin.getJoined_squid_id))
      joinTypes.add(JoinType.parse(squidJoin.getJoinType))
      joinExpressions.add(squidJoin.getJoin_Condition)
      squidNames.add(squid.getName)
      val cs: util.List[Column] = TranslateUtil.getColumnsFromSquid(squid)
      val i2c: util.Map[Integer, TStructField] = new util.HashMap[Integer, TStructField]
      import scala.collection.JavaConversions._
      for (c <- cs) {
        i2c.put(c.getId, new TStructField(c.getName, TDataType.sysType2TDataType(c.getData_type), c.isNullable, c.getPrecision, c.getScale))
      }
      id2Columns.add(i2c)

    }
    val sJoinSquid = new SJoinSquid(preSSquids, joinTypes, joinExpressions, squidNames, id2Columns)
    List(sJoinSquid)
  }

  def translateUnionSquid(squidJoins: JList[SquidJoin]): List[ESquid] = {
    val squids = new ListBuffer[ESquid]()

    import scala.collection.JavaConversions._

    var leftESquid:ESquid = null
    var leftInKeyList:List[Integer] = null
    squidJoins.toList.map(sj => {
      if(sj.getJoinType == JoinType.BaseTable.value()) {
        leftESquid = context.getSquidOut(sj.getJoined_squid_id)
        leftInKeyList = context.getSquidById(sj.getJoined_squid_id)
          .asInstanceOf[DataSquid]
          .getColumns.toList.sortWith((c1, c2) => {
            c1.getRelative_order - c2.getRelative_order > 0
          })
          .map(c => {
            c.getId.asInstanceOf[Integer]
          })
      } else {
        assert(leftESquid != null, "union 左边不能为空")

        val joinType = if(sj.getJoinType == JoinType.Unoin.value()) {
          TUnionType.UNION
        } else {
          TUnionType.UNION_ALL
        }

        val rightInKeyList: List[Integer] = context.getSquidById(sj.getJoined_squid_id)
          .asInstanceOf[DataSquid]
          .getColumns.toList.sortWith((c1, c2) => {
            c1.getRelative_order - c2.getRelative_order > 0
          }).map(c => {
            c.getId.asInstanceOf[Integer]
          })

        require(leftInKeyList.size == rightInKeyList.size, "union 操作,两边数据集的列数必须一致")

        val rightESquid = context.getSquidOut(sj.getJoined_squid_id).asInstanceOf[SSquid]

        val unionESquid = new SUnionSquid(joinType,
          leftESquid.asInstanceOf[SSquid] ,
          rightESquid,
          leftInKeyList, rightInKeyList)

        unionESquid.preSquids = List(leftESquid, rightESquid)

        squids.append(unionESquid)
        leftESquid = unionESquid
      }
    })

    squids.toList
  }
}
