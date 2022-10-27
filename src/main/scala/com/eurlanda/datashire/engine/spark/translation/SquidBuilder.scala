package com.eurlanda.datashire.engine.spark.translation

import com.eurlanda.datashire.engine.entity.ESquid
import com.eurlanda.datashire.engine.spark.stream.SSquid
import com.eurlanda.datashire.entity.Squid

import scala.collection.mutable.ListBuffer

/**
  * Created by zhudebin on 16/1/21.
  */
trait SquidBuilder {
  def translate():List[ESquid]
  val currentSquid:Squid

  val currentBuildedSquids = new ListBuffer[ESquid]

  /**
    * 获取当前squid 翻译出来的squid 链的最后一个
    * @return
    */
  def getCurrentLastSSquid : SSquid = {
    if(currentBuildedSquids.size == 0) {
      return null
    }
    currentBuildedSquids.filter(_.isInstanceOf[SSquid]).last.asInstanceOf[SSquid]
  }

  /**
    * 获取当前squid 翻译出来的squid 链的最后一个
    * @return
    */
  def getCurrentLastSSquid(context: StreamBuilderContext) : SSquid = {
    val currentSSquid = getCurrentLastSSquid
    if(currentSSquid == null) {
      val preSquids = context.getPrevSquids(currentSquid)
      require(preSquids!=null && preSquids.size==1, "找不到前置squid")
      context.getSquidOut(preSquids(0).getId).asInstanceOf[SSquid]
    } else {
      currentSSquid
    }
  }
}
