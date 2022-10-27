package com.eurlanda.datashire.engine.spark.mllib

import org.apache.commons.logging.LogFactory
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by zhudebin on 14-6-7.
 */
object TrainDataConvertor {

    val log = LogFactory.getLog(TrainDataConvertor.getClass)

    def convertLabledPoint(csn: String): LabeledPoint = {
        // todo 是否能为空
//        log.debug(csn)
        val arr = csn.split(",")
        val fea = for(i <- 1 until arr.length) yield arr(i).toDouble
        LabeledPoint(arr(0).toDouble, Vectors.dense(fea.toArray))
    }

    def convertRating(csn: String): Rating = {
        val arr = csn.split(",")
        if(arr.size !=3){
            throw new RuntimeException("组合数据个数不是3，第一个元素是整数，第二个元素是整数，第三个元素是数字")
        }
        try {
            Rating(arr(0).toInt, arr(1).toInt, arr(2).toDouble)
        }catch {
            case e:Throwable => {
               throw new RuntimeException("数据类型不满足要求：第一个元素是整数，第二个元素是整数，第三个元素是数字")
            }
        }
       /* if(arr.size >1 && arr.size < 3) {
            Rating(arr(0).toInt, arr(1).toInt, 0.0d)
        } else {
            Rating(arr(0).toInt, arr(1).toInt, arr(2).toDouble)
        } */
    }

    def convertDoubleArray(csn: String): Array[Double] = {
        val arr = csn.split(",")
        (for(i <- 0 until arr.length) yield arr(i).toDouble).toArray
    }

    /**
     * 将csn 转化为 VECTOR
     * @param csn
     * @return
     */
    def convertDoubleVector(csn: String): Vector = {
        Vectors.dense(csn.split(",").map(_.toDouble))
    }

    /**
      * 将csn 转化为 VECTOR
      * @param csn
      * @return
      */
    def convertMLDoubleVector(csn: String): org.apache.spark.ml.linalg.DenseVector = {
        org.apache.spark.ml.linalg.Vectors.dense(csn.split(",").map(_.toDouble)).toDense
    }

    /**
      * 将csv 转化为 Tuple8
      * 用于关联规则查询
      * @param csv
      * @return
      */
    def convertTuple8(csv: String):Tuple8[String, String, Double, Double, Double,Int, Int, Int] = {
        val arr = csv.split(",")
        new Tuple8(arr(0), arr(1), arr(2).toDouble, arr(3) toDouble, arr(4) toDouble, arr(5).toInt, arr(6).toInt, arr(7).toInt)
    }

}
