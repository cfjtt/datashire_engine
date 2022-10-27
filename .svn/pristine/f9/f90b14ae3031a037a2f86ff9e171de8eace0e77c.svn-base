package com.eurlanda.datashire.engine.spark.translation

import java.util
import java.util.{Map => JMap}

import com.eurlanda.datashire.adapter.HyperSQLManager
import com.eurlanda.datashire.engine.entity._
import com.eurlanda.datashire.engine.spark.stream._
import com.eurlanda.datashire.engine.translation.{BuilderContext, IdKeyGenerator}
import com.eurlanda.datashire.engine.util.ConstantUtil
import com.eurlanda.datashire.entity._
import com.eurlanda.datashire.entity.dest.{DestESSquid, DestHDFSSquid, DestImpalaSquid, DestSquid}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by zhudebin on 16/1/21.
  */
class StreamBuilderContext(val squidFlow:SquidFlow) {

  var project:Project = null
  var taskId:String = null
  var jobId:Int = 0
  var repositoryId:Int = 0

  val idKeyGenerator:IdKeyGenerator = new IdKeyGenerator
  var variable: JMap[String, DSVariable] = new util.HashMap[String, DSVariable]()

  val id2squid:Map[Int, Squid] = squidFlow.getSquidList.map(s => {
    (s.getId, s)
  }).toMap

  val name2squid = squidFlow.getSquidList.map(s => {
    (s.getName, s)
  }).toMap

  // 已经 build 的 squid
  val buildSquids = new mutable.HashMap[Squid,List[ESquid]]()
  // 翻译出来的 SSquid
  val allBuildedSquids = new ListBuffer[ESquid]()

  def build(): SSquidFlow = {

    def initIdKeyGenerator(dataSquid: DataSquid) {
      for(column <- dataSquid.getColumns.toList) {
        idKeyGenerator.addExcludeKey(column.getId)
      }
    }

    // 初始运行参数
    val ds = ConstantUtil.getSysDataSource
    val sql = "select * from ds_project where id=" + squidFlow.getProject_id
    this.project = HyperSQLManager.query2List(ds.getConnection, true, sql, null, classOf[Project]).get(0)
    this.repositoryId = project.getRepository_id

    // 先翻译 批处理 squid
    val builderContext = new BuilderContext(1, squidFlow)
    val tSquidFlow = builderContext.build()

    tSquidFlow.getJobContext.setSquidFlowId(squidFlow.getId)
    tSquidFlow.getJobContext.setProjectId(squidFlow.getProject_id)

    // 需要将其中 dest 落地squid,并且上游为流式squid的过滤出来

    val destSquids = builderContext.getBuildedMapper.toMap.filter(t2 => {
      var flag = t2._1.isInstanceOf[DestSquid]

      if(flag) {
        val preSquid = builderContext.getPrevSquids(t2._1)
        if(preSquid != null && preSquid.size() > 0) {
          flag = preSquid(0).isInstanceOf[StreamSquid]
        } else {
          // 没有上游squid, 同样需要过滤出来
          flag = true
        }
      }
      flag
    })

    allBuildedSquids.prependAll(tSquidFlow.getSquidList.filter(s => {
      !destSquids.exists(t2 => {
        t2._2.contains(s)
      })
    }))
    builderContext.getBuildedMapper.toMap.filter(t2 => {
      !t2._1.isInstanceOf[StreamSquid]
    }).foreach(t2 => {
      val tSquids = t2._2.toList.map(ts => {
        ts.setCurrentFlow(tSquidFlow)
        ts
      })
      buildSquids.put(t2._1, tSquids)
    })

    // 初始化 idKeyGenerator
    for(squid <- squidFlow.getSquidList.toList) {
      if(squid.isInstanceOf[DataSquid]) {
        initIdKeyGenerator(squid.asInstanceOf[DataSquid])
      }
    }

    // 开始翻译
    for(squid <- squidFlow.getSquidList.toList) {
      build(squid)
    }

    buildDestStreamSquid(destSquids)
    new SSquidFlow(allBuildedSquids.toList, squidFlow.getName)
  }

  /**
    * 将流式squid下游的dest squid 翻译
    */
  def buildDestStreamSquid(destStreamSquids:Map[Squid, util.List[TSquid]]) {
    destStreamSquids.foreach(t2 => {
      val preSquids = this.getPrevSquids(t2._1)
      if(preSquids != null && preSquids.size == 1) {
        val preOutSquid = this.getSquidOut(preSquids(0).getId)
        if(t2._1.isInstanceOf[DestESSquid]) {
          val tDestESSquid = t2._2.get(0).asInstanceOf[TDestESSquid]
          val destEs = new SDestESSquid(tDestESSquid.getPath, tDestESSquid.getEs_nodes, tDestESSquid.getEs_port, tDestESSquid.getId2name, tDestESSquid.getIs_mapping_id)
          destEs.preSquids = List(preOutSquid)
          buildSquids.put(preSquids(0), List(destEs))
          allBuildedSquids.append(destEs)
        } else if(t2._1.isInstanceOf[DestHDFSSquid]) {
          val tHDFSSquid = t2._2.get(0).asInstanceOf[THdfsPartitionFallSquid]
          val destHdfs = new SDestHDFSSquid(tHDFSSquid.fallColumnNames, tHDFSSquid.fallColumnIds, tHDFSSquid.partitionColumnIds, tHDFSSquid.params)
          destHdfs.preSquids = List(preOutSquid)
          buildSquids.put(preSquids(0), List(destHdfs))
          allBuildedSquids.append(destHdfs)
        } else if(t2._1.isInstanceOf[DestImpalaSquid]) {
          val tDestImpalaSquid = t2._2.get(0).asInstanceOf[TDestImpalaSquid]
          val destImpala = new SDestImpalaSquid(tDestImpalaSquid.getDataSource, tDestImpalaSquid.getColumns)
          destImpala.preSquids = List(preOutSquid)
          buildSquids.put(preSquids(0), List(destImpala))
          allBuildedSquids.append(destImpala)
        }
      }
    })
  }

  def build(squid: Squid) {
    // 验证其之前的squid是否翻译了
    val preSquids = getPrevSquids(squid)

    for(preSquid <- preSquids) {
      if(!buildSquids.contains(preSquid)) {
        build(preSquid)
      }
    }

    if(!buildSquids.contains(squid)) {

      val builder = if(squid.isInstanceOf[KafkaExtractSquid]) {
        new KafkaExtractBuilder(this, squid)
      } else if(squid.isInstanceOf[StreamStageSquid]) {
        new StreamStageBuilder(this, squid)
      }
      else {
        null
      }

      if(builder != null) {
        val squids = builder.translate()
        allBuildedSquids.appendAll(squids)
        buildSquids.put(squid, squids)
      } else {
        println("不存在该squid 类型 " + squid.getClass)
        buildSquids.put(squid, null)
      }
    }
  }

  /**
    * 获得 @param squid 之前的squid集合
    *
    * @param squid
    * @return
    */
  def getPrevSquids(squid: Squid):List[Squid] = {
    // 过滤 link
    squidFlow.getSquidLinkList.filter(sl => {
      sl.getTo_squid_id == squid.getId
    }).map(_.getFrom_squid_id)
      .distinct
      .map(id => id2squid.getOrElse(id, null))
      .filter(_ != null)
      .toList

  }

  def getSquidById(squidId: Int): Squid = {
    id2squid.getOrElse(squidId, null)
  }

  /**
    * 通过squidId,获取该squid对应翻译后的最后一个SSquid
    *
    * @param prevSquidId
    * @return
    */
  def getSquidOut(prevSquidId: Int): ESquid = {
    var sSquids = buildSquids.get(getSquidById(prevSquidId)).getOrElse(List[ESquid]())
    if(sSquids == null || sSquids.size == 0) {
      build(getSquidById(prevSquidId))
      sSquids = buildSquids.get(getSquidById(prevSquidId)).getOrElse(List[ESquid]())

    }

    if(sSquids !=null && sSquids.size > 0) {
      sSquids.filter(ssquid => {
        (!ssquid.isInstanceOf[SDataFallSquid]) && (!ssquid.isInstanceOf[SDestMongoSquid])
      }).last
    } else {
      null
    }
  }

  def genTJobContext(): TJobContext = {
    val jobContext = new TJobContext

    jobContext.setJobId(jobId)
    jobContext.setProjectId(project.getId)
    jobContext.setProjectName(project.getName)
    jobContext.setRepositoryId(repositoryId)
//    jobContext.setRepositoryName(repositoryName)
    jobContext.setSquidFlowId(squidFlow.getId)
//    jobContext.setSparkSession()
    jobContext.setSquidFlowName(squidFlow.getName)
    jobContext.setTaskId(taskId)

    jobContext
  }
}
