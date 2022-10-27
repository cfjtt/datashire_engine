package com.eurlanda.datashire.engine.spark.mllib.model

class DiscretizeModel(val min:Double, val max:Double, val buckets:Int) extends Serializable {

    require(buckets>0,"DiscretizeModel的最大离散区间数目不能是0")
    // 区间位数 3->1,12->2,234->3
    val points = com.eurlanda.datashire.engine.util.MathUtil.lengthOfInt(buckets)
    // 设置 离散后值的精度，区间位数+1
    val ds:java.text.DecimalFormat = new   java.text.DecimalFormat("#." + makeString(points+1, "#"))
    // 每个区间对应的值
    val par = ds.format(BigDecimal(1) / (buckets-1)).toDouble

    // 区间数
    val partitions = for(i <- 0 to buckets) yield {
      if(i == buckets -1) {
        1
      } else {
        i * par
      }
    }

    // 每个区间大小
    val section = (max - min) / buckets
    def makeString(len:Int, str:String) = {
        val arr =  new Array[String](len)
        for(i <- 0 until len) {
            arr(i) = str
        }
        arr.mkString("")
    }

    def predict(testData: Double): Double = {
//        ((testData - min)/section).floor.toInt * par
      if(section ==0){
         return testData
      }
      var preIdx = buckets - 1 - ((BigDecimal(max) - testData)/section).toFloat.floor.toInt
      if(preIdx < 0) {
        preIdx = 0
      }
      if(preIdx>=partitions.size){
        throw new RuntimeException("数组越界，数组最大下标"+partitions.size+",实际下标"+preIdx)
      }
      partitions(preIdx)
    }

}

