package com.eurlanda.datashire.engine.spark.stream

import java.util
import java.util.{List => JList, Map => JMap}

import com.eurlanda.datashire.engine.entity.TTransformationSquid._
import com.eurlanda.datashire.engine.entity._
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType
import com.eurlanda.datashire.engine.exception.TransformationException
import com.eurlanda.datashire.engine.util.{ConstantUtil, EngineLogFactory, SerializeUtil, SquidUtil}
import com.eurlanda.datashire.enumeration.{SquidTypeEnum, TransformationTypeEnum}
import org.apache.commons.collections.CollectionUtils
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by zhudebin on 16/3/14.
  */
class STransformationSquid(var tTransformationActions:util.ArrayList[TTransformationAction] = null,
                           var isPreviousExceptionSquid: Boolean = false,
                           var jobContext: TJobContext = null) extends SSquid {

  var squidId:Int = 0

  override var preSquids: List[ESquid] = _

  override def run(sc: StreamingContext): Any = {
    val preSquid = preSquids(0)

    // 预处理数据挖掘模型
    preProcess(new JavaSparkContext(sc.sparkContext))
    val taBytes = SerializeUtil.serialize(tTransformationActions)
    val tTransformationActionBytes_br = sc.sparkContext.broadcast(taBytes)

    val isPreviousExceptionSquid_br = sc.sparkContext.broadcast(isPreviousExceptionSquid)
    val jobContext_br = sc.sparkContext.broadcast(jobContext)

    outDStream = preSquid.asInstanceOf[SSquid].outDStream.transform(rdd => {

      rdd.map[util.Map[Integer, DataCell]](m => {
        import scala.collection.JavaConversions._
        val jm:util.HashMap[Integer, DataCell] = new util.HashMap[Integer, DataCell]()

        m.entrySet().toList.foreach(entry => {
          jm.put(entry.getKey, entry.getValue)
        })

        val actions = SerializeUtil.deserializeTransformatonActions(tTransformationActionBytes_br.value)
        val isPreExcep = isPreviousExceptionSquid_br.value
        val jc = jobContext_br.value

        for(action <- actions.toList) {
          if(!jm.containsKey(TTransformationSquid.ERROR_KEY) || isPreExcep) {
            try {
              action.gettTransformation.process(jm)
              if (CollectionUtils.isNotEmpty(action.getRmKeys)) {
                for (i <- action.getRmKeys.toList) {
                  jm.remove(i)
                }
              }
            } catch {
              case e:Exception =>
                EngineLogFactory.logError(jc, new TransformationException(jm, action.gettTransformation, e))
                if (m.size != 0) {
                  jm.put(ERROR_KEY, new DataCell(TDataType.STRING, e.getMessage))
                }
            }
          }
        }

//        println("---------------" + jm)
        jm
      }).filter(m => {
        !m.containsKey(ERROR_KEY)
      })
//      SquidUtil.transformationRDD(rdd.toJavaRDD(), tTransformationActions_br.value, isPreviousExceptionSquid_br.value, jobContext_br.value).rdd
    })
  }

  def preProcess(jsc: JavaSparkContext): JList[TTransformationAction] = {
    import scala.collection.JavaConversions._

    for(ta <- tTransformationActions.toList) {
      val transformationType = ta.gettTransformation().getType
      if(transformationType == TransformationTypeEnum.PREDICT
        || transformationType == TransformationTypeEnum.INVERSEQUANTIFY) {

        // 处理 predict, 根据查询SQL 查询出MODEL对象
        if(ta.gettTransformation().getInKeyList.size() == 1) {
          val sql = ta.gettTransformation().getInfoMap.get(TTransformationInfoType.PREDICT_DM_MODEL.dbValue).toString
          val squidType = ta.gettTransformation().getInfoMap.get(TTransformationInfoType.PREDICT_MODEL_TYPE.dbValue).asInstanceOf[SquidTypeEnum]
          val result:JMap[String, Object] = ConstantUtil.getDataMiningJdbcTemplate.queryForMap(sql)
          val model = jsc.broadcast(SquidUtil.genModelFromResult(result, squidType))
          ta.gettTransformation().getInfoMap.put(TTransformationInfoType.PREDICT_DM_MODEL.dbValue, model)
        }

      }
    }

    tTransformationActions
  }

  @transient
  override var outDStream: DStream[JMap[Integer, DataCell]] = _
}
