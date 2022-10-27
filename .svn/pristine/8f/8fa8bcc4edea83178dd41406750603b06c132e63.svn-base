package com.eurlanda.datashire.engine.spark.translation

import com.eurlanda.datashire.engine.dao.SSquidFlowDao
import com.eurlanda.datashire.engine.util.ConstantUtil
import com.eurlanda.datashire.entity.SquidFlow

/**
  * Created by zhudebin on 16/1/26.
  */
object StreamTranslator {

  def main(args: Array[String]) {

    val squidFlowId = 299


    // 查询 流式 squidflow
    val dao: SSquidFlowDao = ConstantUtil.getSSquidFlowDao()
    val squidFlow: SquidFlow = dao.getSSquidFlow(squidFlowId)

    // 翻译
    val ss = new StreamBuilderContext(squidFlow).build()

    println(ss)
  }

}
