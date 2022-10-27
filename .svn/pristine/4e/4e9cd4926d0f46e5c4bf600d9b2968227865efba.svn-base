package com.eurlanda.datashire.engine.spark

import com.eurlanda.datashire.engine.spark.mllib.model.DiscretizeModel

/**
  * Created by zhudebin on 16/6/24.
  */
object DiscretizeModelTest {

  def main(args: Array[String]) {
    val dm = new DiscretizeModel(33, 103, 4)

    // [103 ~ 85.5),[85.5 ~ 68), [68 ~ 50.5), [50.5 ~ 33]

    println(33 + "-" + dm.predict(33))
    println(50 + "-" + dm.predict(50))
    println(51 + "-" + dm.predict(51))
    println(59 + "-" + dm.predict(59))
    println(79 + "-" + dm.predict(79))
    println(99 + "-" + dm.predict(99))
    println(103 + "-" + dm.predict(103))
  }

}
