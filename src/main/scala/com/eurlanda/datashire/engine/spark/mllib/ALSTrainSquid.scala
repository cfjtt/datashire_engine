package com.eurlanda.datashire.engine.spark.mllib

import java.util.{Map => JMap}

import com.eurlanda.datashire.engine.entity.{DataCell, TDataType, TSquid}
import com.eurlanda.datashire.engine.exception.{EngineException, EngineExceptionType}
import org.apache.spark.CustomJavaSparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.recommendation.{ALS, ALSTrainModel, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
 *
 * @param preRDD
 * @param inKey
 * @param percentage
 * @param rank
 * @param lambda
 * @param implicitPrefs
 * @param alpha
 * @param iterations
 * @param numBlocks
 */
class ALSTrainSquid(preRDD:JavaRDD[JMap[Integer, DataCell]], inKey: Integer,
                    percentage:Float,
                    val rank:Int = 1,
                    val lambda:Double = 0.01,
                    val implicitPrefs:Boolean = false,
                    val alpha:Double = 1.0,
                    val iterations:Int = 10,
                    val numBlocks:Int = 10,
                    val seed:Long = 1l) extends Serializable {

   def run(jsc: CustomJavaSparkContext):(ALSTrainModel, java.lang.Float, java.lang.Long) = {
      if(preRDD == null || preRDD.isEmpty()){
       throw new RuntimeException("数据错误，可能原因：" +
         "ALS训练组合不是3数，第一个元素是整数，第二个元素是整数，第三个元素是数字"
       )
     }
      val dataRDD = preRDD.rdd.map(m => {
           val dc: DataCell = m.get(inKey)
           if (dc.getdType ne TDataType.RATING) throw new EngineException(EngineExceptionType.DATA_TYPE_ILLEGAL)
           dc.getData.asInstanceOf[Rating]
       })

       // 总数
       val total = dataRDD.count()
       if(total * percentage < 1) {
         throw new RuntimeException("数据集总数为:" + total + ",训练数据比例为:" + percentage  + ",参与训练数据总量少于1条")
       }
       // 训练数据，采用随机 近似占比
       val splits = dataRDD.randomSplit(Array(percentage.toDouble, (1-percentage.toDouble)))
       val trainRDD = splits(0).cache()

       //numBlocks: Int, var rank: Int, var iterations: Int,
       // var lambda: Double,
       //var implicitPrefs: Boolean, var alpha: Double
       // 训练模型
       val model = implicitPrefs match {
           case true =>
               ALS.trainImplicit(trainRDD, rank, iterations, lambda, numBlocks, alpha, seed)
           case false =>
               ALS.train(trainRDD, rank, iterations, lambda, numBlocks, seed)
       }

        // 测试数据,如果训练比例是100%，则测试数据用训练数据
       val testRDD =
       if(percentage==1.0){
          trainRDD
        }else{
          splits(1)
        }

       (new ALSTrainModel(model), computeSuc(model, testRDD, implicitPrefs), total)
   }

    /** Compute RMSE (Root Mean Squared Error). */
   private def computeSuc(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean) = {

        val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
        val predictionsAndRatings = predictions.map{ x =>
            ((x.user, x.product), x.rating)
        }.join(data.map(x => ((x.user, x.product), x.rating))).values
        var precision = 0.0f
        if(implicitPrefs) {
          if (! predictionsAndRatings.isEmpty()) {
            val totalSuc = predictionsAndRatings.map(x => if (x._1 == x._2) 1 else 0).reduce(_ + _)
            val total = data.count()
            precision = totalSuc.toFloat / total
          }
        }
        precision
    }
}
