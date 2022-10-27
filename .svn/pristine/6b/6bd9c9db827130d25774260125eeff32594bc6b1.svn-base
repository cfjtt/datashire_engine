package org.apache.spark.ml.regression

import java.util.{Arrays, UUID}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasMaxIter, HasTol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, RowMatrix, _}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * 偏最小二乘的一个数据
  *
  * @param yLabel   Y值  有多个值,label
  * @param xFeature X 有多个值.feature
  */
case class PartialLeastSquaresSample(yLabel:org.apache.spark.ml.linalg.Vector,
                                       xFeature: org.apache.spark.ml.linalg.Vector)

/**
  * Created by Administrator on 2017-06-26.
  * pls 需要的参数
  */
trait PartialLeastSquaresRegressionParams extends  PredictorParams
      with HasMaxIter with HasTol  {

  /**
    * 主成分数
 *
    * @group param
    */
  final val componentCount: IntParam = new IntParam(this, "componentCount", "主成分数要大于0", ParamValidators.gt(0))

  /** @group getParam */
  def getComponentCount: Int = $(componentCount)

  /**
    * X每列均值
 *
    * @group param
    */
  final val XNormalizerModelColumnMean: DoubleArrayParam = new DoubleArrayParam(this, "XNormalizerModelColumnMean",
                                                            "X标注化模型每列均值不能是空", ParamValidators.arrayLengthGt(0))

  /** @group getParam */
  def getXNormalizerModelColumnMean: Array[Double] = $(XNormalizerModelColumnMean)

  /**
    * X每列标准差
 *
    * @group param
    */
  final val XNormalizerModelColumnStd: DoubleArrayParam = new DoubleArrayParam(this, "XNormalizerModelColumnStd",
                                        "X标准差不能是空", ParamValidators.arrayLengthGt(0))

  /** @group getParam */
  def getXNormalizerModelColumnStd: Array[Double] = $(XNormalizerModelColumnStd)

  /**
    * Y每列均值
 *
    * @group param
    */
  final val YNormalizerModelColumnMean: DoubleArrayParam = new DoubleArrayParam(this, "YNormalizerModelColumnMean",
                            "Y每列均值不能是空", ParamValidators.arrayLengthGt(0))

  /** @group getParam */
  def getYNormalizerModelColumnMean: Array[Double] = $(YNormalizerModelColumnMean)

  /**
    * Y每列标准差
 *
    * @group param
    */
  final val YNormalizerModelColumnStd: DoubleArrayParam = new DoubleArrayParam(this, "YNormalizerModelColumnStd",
                          "Y每列标准差不能是空", ParamValidators.arrayLengthGt(0))

  /** @group getParam */
  def getYNormalizerModelColumnStd: Array[Double] = $(YNormalizerModelColumnStd)


  /**
    * Y每列标准差
 *
    * @group param
    */
  final val trainPercentage: DoubleParam = new DoubleParam(this, "trainPercentage", "训练集比例在区间（0,1.0]",
                                 ParamValidators.inRange(0.0,1.0))

  /** @group getParam */
  def getTrainPercentage: Double = $(trainPercentage)

}

/**
  * pls训练类
  */
class PartialLeastSquaresRegression (override val uid: String)
  extends Regressor[org.apache.spark.ml.linalg.DenseVector,PartialLeastSquaresRegression,PartialLeastSquaresRegressionModel]
    with PartialLeastSquaresRegressionParams with Logging {

  def this() = this(Identifiable.randomUID("PartialLeastSquaresRegression"))

  /**
    * 最大迭代次数
 *
    * @param value
    * @return
    */
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  /**
    * 容许误差
 *
    * @param value
    * @return
    */
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 0.000001)

  /**
    * 主成分数
    */
  def setComponentsCount(value :Int):this.type  = set(componentCount,value)
  setDefault(componentCount -> 1)

  /**
    * X每列均值
    */
  def setXNormalizerModelColumnMean(value :Array[Double]):this.type  = set(XNormalizerModelColumnMean,value)

  /**
    * X每列标准差
    */
  def setXNormalizerModelColumnStd(value :Array[Double]):this.type  = set(XNormalizerModelColumnStd,value)

  /**
    * Y每列均值
    */
  def setYNormalizerModelColumnMean(value :Array[Double]):this.type  = set(YNormalizerModelColumnMean,value)

  /**
    * X每列标准差
    */
  def setYNormalizerModelColumnStd(value :Array[Double]):this.type  = set(YNormalizerModelColumnStd,value)

  /**
    * 训练集比例
    */
  def setTrainPercentage(value :Double):this.type  = set(trainPercentage,value)


  private var sampleCount = 0  // total_dataset
  private var XTrainColumnCount = 0
  private var YTrainColumnCount = 0
  private var model: PartialLeastSquaresRegressionModel = null
  private val algorithm = "nipals" // nipals， svd

  override  def train(dataset: Dataset[_]):  PartialLeastSquaresRegressionModel={
     throw new RuntimeException("请调用train2")
  }

  /**
    * 训练
    * 训练数据需要 Standard 标准化到 均值是0，
    * @param dataset 训练前要把 X,Y合并在一个dataframe中,X，Y都是CSN
    * @return
    *   (sampleCount,model)
    */
  def train2(dataset: Dataset[_]): (Long, PartialLeastSquaresRegressionModel)= {
    import org.apache.spark.ml.regression.PartialLeastSquaresRegression._
    try {
      val processNaDf = dataset.na.drop().persist(StorageLevel.MEMORY_AND_DISK)
      sampleCount = processNaDf.count().toInt
      if (sampleCount == 0) {
        throw new RuntimeException("没有训练数据或数据是空")
      }
      if (getTrainPercentage <= 0.0 || getTrainPercentage > 1.0) {
        throw new RuntimeException("训练集比例应在区间(0,1]")
      }
      var trainDataCount = 0
      var trainData = processNaDf
      if (getTrainPercentage == 1.0) {
        trainDataCount = sampleCount
      } else {
        val trainTestData = processNaDf.randomSplit(Array(getTrainPercentage, 1.0 - getTrainPercentage))
        trainData = trainTestData.apply(0)
        trainDataCount = trainData.count().toInt
      }
      if (trainDataCount == 0) {
        throw new RuntimeException("没有训练数据，可能原因：数据太少或训练集比例设置过大或过小")
      }
      val YX = getYX(trainData)
      val Y = YX._1
      val X = YX._2
      XTrainColumnCount = X.numCols().toInt
      YTrainColumnCount = Y.numCols().toInt
      if (getXNormalizerModelColumnMean.length != XTrainColumnCount) {
        throw new RuntimeException("X inputs与X标准化模型的列数不相同")
      }
      if (getYNormalizerModelColumnMean.length != YTrainColumnCount) {
        throw new RuntimeException("Y inputs与Y标准化模型的列数不相同")
      }
      val componentcount = getComponentCount
      model = new PartialLeastSquaresRegressionModel(sampleCount, trainDataCount, XTrainColumnCount, YTrainColumnCount,
        componentcount, getXNormalizerModelColumnMean, getXNormalizerModelColumnStd, getYNormalizerModelColumnMean,
        getYNormalizerModelColumnStd, UUID.randomUUID().toString)

      val sparkContext = dataset.rdd.sparkContext
      model.xWeight = initCoordinateMatrix(sparkContext, XTrainColumnCount, componentcount, 0.0)
      model.yWeight = initCoordinateMatrix(sparkContext, YTrainColumnCount, componentcount, 0.0)
      model.xScore = initCoordinateMatrix(sparkContext, trainDataCount, componentcount, 0.0)
      model.yScore = initCoordinateMatrix(sparkContext, trainDataCount, componentcount, 0.0)
      model.xLoading = initCoordinateMatrix(sparkContext, XTrainColumnCount, componentcount, 0.0)
      model.yLoading = initCoordinateMatrix(sparkContext, YTrainColumnCount, componentcount, 0.0)
      model.xRotation = initCoordinateMatrix(sparkContext, XTrainColumnCount, componentcount, 0.0)
      model.yRotation = initCoordinateMatrix(sparkContext, YTrainColumnCount, componentcount, 1.0)
      model.coefficient = initCoordinateMatrix(sparkContext, XTrainColumnCount, YTrainColumnCount, 0.0)

      var Xk = X
      var Yk = Y

      if (componentcount < 1 || componentcount > XTrainColumnCount) {
        throw new RuntimeException("K值应 >=1 且<= X的列数 " + XTrainColumnCount)
      }
      if (getMaxIter < 1) {
        throw new RuntimeException("迭代次数应大于0")
      }
      for (i <- 0 until (componentcount)) {
        val ykZeroValues = Yk.toRowMatrix().computeColumnSummaryStatistics().normL1.toArray.filter(v => v == 0.0)
       // val ykZeroValues = getColumnNormL1(Yk).filter(v => v == 0.0)
        if (ykZeroValues.length == Yk.numCols()) { //全是0
          throw new RuntimeException("数据异常")
        }

        var xweight: CoordinateMatrix = null // 列向量(p*1)
        var yweight: CoordinateMatrix = null // 列向量(p*1)

        val transposedXk = Xk.transpose()
        val transposedYk = Yk.transpose()
        val transposedXkBlockMatrix = transposedXk.toBlockMatrix().persist(StorageLevel.MEMORY_AND_DISK)
        val transposedYkBlockMatrix = transposedYk.toBlockMatrix().persist(StorageLevel.MEMORY_AND_DISK)
        val XkBlockMatrix = Xk.toBlockMatrix().persist(StorageLevel.MEMORY_AND_DISK)
        val YkBlockMatrix = Yk.toBlockMatrix().persist(StorageLevel.MEMORY_AND_DISK)

        if (algorithm.trim.equalsIgnoreCase("nipals")) {
          val xyweight = nipalsTwoblocksInnerLoop(dataset.sparkSession.sparkContext, Xk, Yk, transposedXk, transposedYk,
                        XkBlockMatrix,YkBlockMatrix, transposedXkBlockMatrix,transposedYkBlockMatrix)
          xweight = xyweight._1
          yweight = xyweight._2
        } else if (algorithm.trim.equalsIgnoreCase("svd")) {
          // todo svd方法暂不用
         //  val xyweight = svdCrossProduct(Xk, Yk)
          //   xweight = xyweight._1
          //   yweight = xyweight._2
        } else {
          throw new RuntimeException("不存在算法" + algorithm)
        }
        val xysvd = svdFlip(xweight, yweight)
        xweight = xysvd._1
        yweight = xysvd._2

        val x_scores = XkBlockMatrix.multiply(xweight.toBlockMatrix()).toCoordinateMatrix()
        val yss = getColumnVectorSquare(yweight)
        var y_scores = YkBlockMatrix.multiply(yweight.toBlockMatrix()).toCoordinateMatrix()
        y_scores = scaleMatrix(y_scores, 1.0 / yss)
        val  x_scoresdot = getColumnVectorSquare(x_scores)
        if (x_scoresdot == 0.0) {
          throw new RuntimeException("数据异常")
        }
       val x_scoresBlockMatrix = x_scores.toBlockMatrix().persist(StorageLevel.MEMORY_AND_DISK)
        var x_loadings = transposedXkBlockMatrix.multiply(x_scoresBlockMatrix).toCoordinateMatrix()
        x_loadings = scaleMatrix(x_loadings, 1.0 / x_scoresdot)
        val mus = x_scoresBlockMatrix.multiply(x_loadings.transpose().toBlockMatrix())
      //  Xk = XkBlockMatrix.subtract(mus).toCoordinateMatrix() // spark2.1的subtract有bug
        Xk = BlockMatrixUtil.subtract(XkBlockMatrix,mus).toCoordinateMatrix()

        var y_loadings = transposedYkBlockMatrix.multiply(x_scoresBlockMatrix).toCoordinateMatrix()
        y_loadings = scaleMatrix(y_loadings, 1.0 / x_scoresdot)
        val mul2 = x_scoresBlockMatrix.multiply(y_loadings.transpose().toBlockMatrix())
     //   Yk = YkBlockMatrix.subtract(mul2).toCoordinateMatrix() // spark2.1的subtract有bug
         Yk = BlockMatrixUtil.subtract(YkBlockMatrix,mul2).toCoordinateMatrix()

        model.xScore = addMatrixColumn(model.xScore, i, x_scores)
        model.yScore = addMatrixColumn(model.yScore, i, y_scores)
        model.xWeight = addMatrixColumn(model.xWeight, i, xweight)
        model.yWeight = addMatrixColumn(model.yWeight, i, yweight)
        model.xLoading = addMatrixColumn(model.xLoading, i, x_loadings)
        model.yLoading = addMatrixColumn(model.yLoading, i, y_loadings)

      //   transposedXk.entries.unpersist()
       //  transposedYk.entries.unpersist()
         transposedXkBlockMatrix.blocks.unpersist()
         transposedYkBlockMatrix.blocks.unpersist()
         XkBlockMatrix.blocks.unpersist()
         YkBlockMatrix.blocks.unpersist()
         x_scoresBlockMatrix.blocks.unpersist()
      }
      val xmulMat = model.xLoading.transpose().toBlockMatrix().multiply(model.xWeight.toBlockMatrix())
      val xinverseMat = getInverse(xmulMat.toCoordinateMatrix().toRowMatrix()) // 矩阵大小 k*k
      model.xRotation = model.xWeight.toIndexedRowMatrix().multiply(xinverseMat).toCoordinateMatrix()

      val yLoadingTransposeBlockMatrix = model.yLoading.transpose().toBlockMatrix().persist(StorageLevel.MEMORY_AND_DISK)
      if (YTrainColumnCount > 1) {
        val yLoadingyWeightdot = yLoadingTransposeBlockMatrix.multiply(model.yWeight.toBlockMatrix())
        val yLoadingyWeightdotInv = getInverse(yLoadingyWeightdot.toCoordinateMatrix().toRowMatrix()) // 矩阵大小 k*k
        model.yRotation = model.yWeight.toIndexedRowMatrix().multiply(yLoadingyWeightdotInv).toCoordinateMatrix()
      }
      val xystdinvMatrix = getXYStdMatrix(sparkContext) // 矩阵，大小 X列数 * Y列数
      model.coefficient = model.xRotation.toBlockMatrix().multiply(yLoadingTransposeBlockMatrix).toCoordinateMatrix()
      model.coefficient = getElementWiseMultiply(model.coefficient, xystdinvMatrix)
      model.setCoefficientDenseMatrix()

      //计算模型度量
      val yrowmatrix = Y.toRowMatrix()
      val residualsMatrix = getResidualsMatrix(model, X.toRowMatrix(), yrowmatrix)
      val columnSummaryStatistics = residualsMatrix.computeColumnSummaryStatistics()
      val residualsColumnVar = columnSummaryStatistics.variance // 残差的每列的方差
      val yColumnVar = yrowmatrix.computeColumnSummaryStatistics().variance
      val residualsColumnNormL2 = columnSummaryStatistics.normL2.toArray
      val residualsColumnNormL1 = columnSummaryStatistics.normL1.toArray

      val mseArr = getMSE(residualsColumnNormL2, trainDataCount)
      val mse = mseArr.mkString(",")
      val rmse = getRMSE(mseArr).mkString(",")
      val r2 = getR2(residualsColumnNormL2,yColumnVar).mkString(",")
      val mae = getMAE(residualsColumnNormL1, trainDataCount)
      val explainedVariance = getExplainedVariance(residualsColumnVar, yColumnVar)

      val trainSummary = new PartialLeastSquaresRegressionTrainingSummary(mse,rmse,r2,mae,explainedVariance)
      model.trainingSummary = trainSummary
      processNaDf.unpersist(false)
      yLoadingTransposeBlockMatrix.blocks.unpersist()
      (sampleCount, model)
    } catch {
      case e: Throwable => {
        val errmsg = e.getMessage
        log.error(errmsg)
        e.printStackTrace()
        if (errmsg.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源，请稍后重试或在配置文件中调大配置项：" +
            "SPARK_EXECUTOR_MEMORY，SPARK_DRIVER_MEMORY", e)
        }
        throw e
      }
    }
  }

  /**
    * 计算每列的绝对值之和
    * @param rowMatrix
    * @return
    */
  private def getColumnNormL1(rowMatrix:RowMatrix): Array[Double] = {
    val seqOp = (v1: org.apache.spark.mllib.linalg.Vector, v2: org.apache.spark.mllib.linalg.Vector) => {
      val v1abs = v1.toArray.map(x => math.abs(x))
      val v2abs = v2.toArray.map(x => math.abs(x))
      val vector1 = new org.apache.spark.mllib.linalg.DenseVector(v1abs)
      val vector2 = new org.apache.spark.mllib.linalg.DenseVector(v2abs)
      val add = vector1.asBreeze.+(vector2.asBreeze)
      new org.apache.spark.mllib.linalg.DenseVector(add.toArray)
    }
    rowMatrix.rows.reduce(seqOp).toArray
  }

  /**
    * 计算每列向量的长度
    * @param rowMatrix
    * @return
    */
  private def getColumnNormL2(rowMatrix:RowMatrix): Array[Double] = {
    val columnCount = rowMatrix.numCols().toInt
    val seqOp = (v1: org.apache.spark.mllib.linalg.Vector, v2: org.apache.spark.mllib.linalg.Vector) => {
      val v1squre = v1.toArray.map(x => x * x)
      val v2squre = v2.toArray.map(x => x * x)
      val vector1 = new org.apache.spark.mllib.linalg.DenseVector(v1squre)
      val vector2 = new org.apache.spark.mllib.linalg.DenseVector(v2squre)
      val add = vector1.asBreeze.+(vector2.asBreeze)
      new org.apache.spark.mllib.linalg.DenseVector(add.toArray)
    }
    val combOp = (v1: org.apache.spark.mllib.linalg.Vector, v2: org.apache.spark.mllib.linalg.Vector) => {
      val add = v1.asBreeze.+(v2.asBreeze)
      new org.apache.spark.mllib.linalg.DenseVector(add.toArray)
    }
    val initVector: org.apache.spark.mllib.linalg.Vector = new org.apache.spark.mllib.linalg.DenseVector(Array.fill(columnCount)(0.0))
    val squred = rowMatrix.rows.treeAggregate(initVector)(seqOp, combOp).toArray
    squred.map(x=>math.sqrt(x))
  }

  /**  计算列向量 xweights, yweights
    * @return  两个列向量
    */
  def nipalsTwoblocksInnerLoop(sparkContext:SparkContext,Xk: CoordinateMatrix, Yk: CoordinateMatrix,
                               transposedXk:CoordinateMatrix, transposedYk:CoordinateMatrix,
                               XkBlockMatrix:BlockMatrix,YkBlockMatrix :BlockMatrix,
                               transposedXkBlockMatrix:BlockMatrix, transposedYkBlockMatrix:BlockMatrix):
  (CoordinateMatrix, CoordinateMatrix) = {
    import org.apache.spark.ml.regression.PartialLeastSquaresRegression._
    val u1random = Yk.entries.filter(row => row.j == 0) // 从Y中选一列, 列矩阵
    var yscore = new CoordinateMatrix(u1random)
    var xweights: CoordinateMatrix = null
    var yweights: CoordinateMatrix = null
    var xweightsold: CoordinateMatrix = null
    if( u1random.map(ent => math.abs(ent.value)).sum() == 0.0){
   /*  if (u1random.filter(_.value == 0.0).count() == u1random.count()) {  */ // 一列全是0
      xweights = initCoordinateMatrix(sparkContext, XTrainColumnCount, 1, getTol) // 列向量
      yweights = initCoordinateMatrix(sparkContext, YTrainColumnCount, 1, getTol) // 列向量
      return (xweights, yweights)
    } else {
      xweights = initCoordinateMatrix(sparkContext, XTrainColumnCount, 1, 0.0) // 列向量
      yweights = initCoordinateMatrix(sparkContext, YTrainColumnCount, 1, 0.0) //
      xweightsold = initCoordinateMatrix(sparkContext, XTrainColumnCount, 1, 0.0) // 列向量
    }
    val esp = 0.0000001
    for (i <- 0 until (getMaxIter)) {
      val xweightstmp = transposedXkBlockMatrix.multiply(yscore.toBlockMatrix()).toCoordinateMatrix()
      val yscoredot = getColumnVectorSquare(yscore)
      xweights = scaleMatrix(xweightstmp, 1.0 / yscoredot)
      var xweightslensqure = getColumnVectorSquare(xweights)
      if (xweightslensqure < esp) {
        xweights = addMatrix(xweights, esp)
      }
      xweightslensqure = getColumnVectorSquare(xweights)
      xweights = scaleMatrix(xweights, 1.0 / (math.sqrt(xweightslensqure) + esp))

      val xweightsBlockMatrix = xweights.toBlockMatrix()
      val xscore = XkBlockMatrix.multiply(xweightsBlockMatrix)
      val xscoredot = getColumnVectorSquare(xscore)
      yweights = transposedYkBlockMatrix.multiply(xscore).toCoordinateMatrix()
      yweights = scaleMatrix(yweights, 1.0 / (xscoredot))

      val yss =getColumnVectorSquare(yweights)
      val yyweightsdot = YkBlockMatrix.multiply(yweights.toBlockMatrix()).toCoordinateMatrix()
      yscore = scaleMatrix(yyweightsdot, 1.0 / yss)

     // val xweightsdiff = xweightsBlockMatrix.subtract(xweightsold.toBlockMatrix())// 列矩阵
      val xweightsdiff = BlockMatrixUtil.subtract(xweightsBlockMatrix,xweightsold.toBlockMatrix())//spark2.1的subtract有bug
      val xweightsdiffdot = getColumnVectorSquare(xweightsdiff)
      if (xweightsdiffdot <= getTol || YTrainColumnCount == 1) {
        return (xweights, yweights)
      }
      xweightsold = xweights
    }
    return (xweights, yweights)
  }

  /**
    * 列向量的长度的平方
    * @param columnVector
    * @return
    */
 private def getColumnVectorSquare(columnVector: CoordinateMatrix): Double = {
   if(columnVector.entries.take(1).apply(0).j > 0){
     throw new RuntimeException("不是列向量")
   }
  columnVector.entries.mapPartitions(par => par.map(ent => ent.value * ent.value)) // 每个元素的平方
       .reduce((v1, v2) => v1 + v2) // 求和
 }

  /**
    * 列向量的长度的平方
    * @param columnVector
    * @return
    */
  private def getColumnVectorSquare(columnVector: BlockMatrix): Double = {
    if(columnVector.blocks.take(1).apply(0)._2.numCols>1){
      throw new RuntimeException("不是列向量")
    }
    columnVector.blocks.mapPartitions(par => par.flatMap(ent => ent._2.toArray.map(x => x * x))) // 每个元素的平方
      .reduce((v1, v2) => v1 + v2)  // 求和
  }

  def copy( extra :ParamMap): PartialLeastSquaresRegression ={
    val pls = copyValues(new PartialLeastSquaresRegression(uid), extra)
    pls
  }

  /**
    * X ,Y
    *
    * @return  矩阵，大小是 X列数 * Y列数
    */
  def getXYStdMatrix(sc:SparkContext): CoordinateMatrix ={
     // todo  分布式矩阵
     val xstdInv = getXNormalizerModelColumnStd.map(x => {
       if (x == 0.0) {
         0.0
       } else {
         1.0 / x
       }
     })
   /* val entX = sc.makeRDD(xstdInv.toSeq).zipWithIndex().map(stdId =>{
      new MatrixEntry(stdId._2,0,stdId._1)
    })
    val entY = sc.makeRDD(getYNormalizerModelColumnStd.toSeq).zipWithIndex().map(stdId =>{
      new MatrixEntry(stdId._2,0,stdId._1)
    }) */
    val entArr = new ArrayBuffer[MatrixEntry]() //
    for(i <- 0 until(xstdInv.length);j<- 0 until(getYNormalizerModelColumnStd.length) ){
      entArr.append(new MatrixEntry(i,j,xstdInv.apply(i) * getYNormalizerModelColumnStd.apply(j)))
    }
    new CoordinateMatrix(sc.parallelize(entArr)) //大小是 X列数 * Y列数
  }

  /**
    *
    *  残差
    * 每列预测值 Y_ 与真实值Y 的差, 因为有多列，每行都是一个向量
    * @return 返回一个矩阵 行数与训练数据行数相同，列数数Y的列数相同
    */
  def getResidualsMatrix(model :PartialLeastSquaresRegressionModel, X:RowMatrix, Y:RowMatrix) : RowMatrix = {
    val modelbc = X.rows.sparkContext.broadcast(model)
    // 预测值与真实值之差
    val resrdd = X.rows.map(row =>
      modelbc.value.predictLabelVector(new org.apache.spark.ml.linalg.DenseVector(row.toArray), true)) //预测值
      .zip(Y.rows) //真实值
      .map(rowid => {
      val diff = rowid._1.asBreeze.:-(rowid._2.asBreeze) // 预测值与真实值之差
      val vec: org.apache.spark.mllib.linalg.Vector = new org.apache.spark.mllib.linalg.DenseVector(diff.toArray)
      vec
    })
    new RowMatrix(resrdd)
  }

  /**
    *  均方差	,csn	,每个Y都有一个均方差,残差平方和
    *  sum (残差^^2) /n
    * @return
    */
  def getMSE(residualsMatrixColumnNormL2:Array[Double], rowCount:Int): Array[Double] = {
    residualsMatrixColumnNormL2.map(x=>x*x).map[Double,Array[Double]](_ /rowCount )
  }

  /**
    * 均方根
    */
  def getRMSE(MSE:Array[Double]):Array[Double] = {
    MSE.map(x => math.sqrt(x))
  }

  /**
    * 决定系数
    * 值越大，自变量对因变量的解释程度越高，自变量引起的变动占总变动的百分比高。
    * 观察点在回归直线附近越密集。表示相关的方程式参考价值越高；相反，越接近0时，表示参考价值越低。
    * R^^2 = 1-SSres/SStot
    * SSres是残差平方和
    * SStot是 y 的方差
    */
  def getR2(residualsMatrixColumnNormL2:Array[Double],YColumnVar:org.apache.spark.mllib.linalg.Vector) :Array[Double] = {
    val residualSqaure = residualsMatrixColumnNormL2.map(x => x * x)
    val res = new ArrayBuffer[Double]()
    for (i <- 0 until (residualSqaure.length)) {
      var vattmp = YColumnVar.apply(i)
      if (vattmp == 0.0) {
        vattmp = 1.0
      }
      res.append(1.0 - residualSqaure.apply(i) / vattmp)
    }
    res.toArray
  }

  /**
    *  每列预测值与真实值差的绝对值
    */
  def getMAE(residualsMatrixColumnNormL1: Array[Double],rowCount:Int) :String = {
    residualsMatrixColumnNormL1.map(x=>x*x).map(_ /rowCount ).mkString(",")
  }

  /**
    * 解释方差
    * 描述单个变量的方差与总方差的比,
    * explainedVariance_i = 1- variance residuals(yi_))/variance(yi) , 即 1- 残差的方差/真实值方差
    * 每列 y 都有一个值
    * @param residualsColumnVar  残差的每列的方差
    * @param YColumnVar
    * @return  一行csn，个数等于Y的列数，不会很大
    */
  def getExplainedVariance(residualsColumnVar:org.apache.spark.mllib.linalg.Vector,YColumnVar:org.apache.spark.mllib.linalg.Vector):String = {
    val res = new ArrayBuffer[Double]()
    for (i <- 0 until (residualsColumnVar.size)) {
      var yvar = YColumnVar.apply(i)
      if (yvar == 0.0) {
        yvar = 1.0
      }
      res.append(1.0 - residualsColumnVar.apply(i) / yvar)
    }
    res.mkString(",")
  }

}


object PartialLeastSquaresRegression{

  /**
    * 矩阵某列加值
    *
    * @param M
    * @param addColumnIndex
    * @param columnMatrix 列矩阵
    */
  def addMatrixColumn(M:CoordinateMatrix,addColumnIndex:Int,columnMatrix:CoordinateMatrix):
  CoordinateMatrix = {
    val addColumnIndexBc = M.entries.sparkContext.broadcast(addColumnIndex)
    val columnMatrixIdRdd = columnMatrix.entries.map(ent => new MatrixEntry(ent.i, addColumnIndexBc.value, ent.value))
    val ents = M.entries.union(columnMatrixIdRdd).map(ent => ((ent.i, ent.j), ent.value))
      .reduceByKey((v1, v2) => v1 + v2).map(ent => new MatrixEntry(ent._1._1, ent._1._2, ent._2))
    new CoordinateMatrix(ents)
  }

  /**
    * 如果X列绝对值最大是负数，把X,Y变号
    *
    * @param XWeightVector  列向量
    * @param YWeightVector  列向量
    * @return  连个列向量
    */
  def svdFlip(XWeightVector : CoordinateMatrix, YWeightVector: CoordinateMatrix) :(CoordinateMatrix, CoordinateMatrix)= {
    val xvalues = XWeightVector.toRowMatrix().rows.map(row => row.apply(0))
    val min = xvalues.min // 列的最小值
    val max = xvalues.max
    if (min < 0 && math.abs(min) > math.abs(max)) {
      val X1 = scaleMatrix(XWeightVector, -1.0)
      val Y1 = scaleMatrix(YWeightVector, -1.0)
      return (X1, Y1)
    }
    return (XWeightVector, YWeightVector)
  }

  /**
    * 初始化矩阵
    *
    * @param sparkContext
    * @param rowCount
    * @param columnCount
    * @param initValue
    * @return
    */
  def initCoordinateMatrix(sparkContext:SparkContext,rowCount:Long,columnCount:Int,initValue:Double): CoordinateMatrix = {
    val columnCountbc = sparkContext.broadcast(columnCount)
    val initValuebc = sparkContext.broadcast(initValue)
    val pa = sparkContext.range(0, rowCount).mapPartitions(par => {
      par.flatMap(row => Range(0, columnCountbc.value).map(columnId => new MatrixEntry(row, columnId, initValuebc.value)))
    })
    new CoordinateMatrix(pa)
  }

  /**
    *  一个数乘以矩阵 ,a * X
    *
    * @param X
    * @param a
    * @return
    */
  def scaleMatrix(X: CoordinateMatrix, a:Double): CoordinateMatrix = {
    val abc = X.entries.sparkContext.broadcast(a)
    new CoordinateMatrix(X.entries.map(row=> new MatrixEntry(row.i,row.j,row.value * abc.value)))
  }

  /**
    *  一个数乘以矩阵 ,a * X
    *
    * @param X
    * @param a
    * @return
    */
  def scaleMatrix(X: BlockMatrix, a:Double): BlockMatrix = {
    val abc = X.blocks.sparkContext.broadcast(a)
    new BlockMatrix(X.blocks.map(row => (row._1, row._2.map(x => x * abc.value))), X.rowsPerBlock, X.colsPerBlock)
  }

  /**
    * 对应元素相乘
    *
    * @param x
    * @param y
    * @return
    */
  def getElementWiseMultiply(x:CoordinateMatrix,y: CoordinateMatrix): CoordinateMatrix = {
    val ents = x.entries.union(y.entries).map(ent => ((ent.i, ent.j), ent.value))
      .reduceByKey((v1, v2) => v1 * v2).map(ent => new MatrixEntry(ent._1._1, ent._1._2, ent._2))
    new CoordinateMatrix(ents)
  }

  /**
    * 逆矩阵
    * @param X  矩阵大小 k*k，不会很大
    * @return
    */
  def getInverse(X: RowMatrix): org.apache.spark.mllib.linalg.DenseMatrix = {
    val nCoef = X.numCols.toInt
    if(nCoef == 1) {
      var value = X.rows.collect().apply(0).apply(0)
      if(value != 0.0) {
        value = 1.0 / value
      }
      return new org.apache.spark.mllib.linalg.DenseMatrix(1, 1, Array(value))
    }
    val svd = X.computeSVD(nCoef, computeU = true,rCond = 1e-16)
    val invS = org.apache.spark.mllib.linalg.DenseMatrix.diag(
                new org.apache.spark.mllib.linalg.DenseVector(svd.s.toArray.map(x => math.pow(x, -1))))
    val U = new org.apache.spark.mllib.linalg.DenseMatrix(
                 svd.U.numRows().toInt, svd.U.numCols().toInt, svd.U.rows.collect.flatMap(x => x.toArray)) //不会很大
    (svd.V.multiply(invS)).multiply(U)
  }

  /**
    * 获取X, Y矩阵
    *
    * @param dataset
    * @return
    */
  def getYX(dataset: Dataset[_]): (CoordinateMatrix,CoordinateMatrix) = {
    val yxsplitedRdd = dataset.select("yx").rdd.zipWithIndex().mapPartitions(par => {
      par.map(row => {
        val yx = row._1.toString.trim.replace("[", "").replace("]", "").split(";") // 两个csn用分号分割,已在TRAIN变换中将其组合,
        (row._2, yx)  // 行id,ycsn,xcsn
      })
    })
    val xinstanceRdd = yxsplitedRdd.mapPartitions(par => {
      par.flatMap(row => {  // 第一个inputs是y, 第二个inputs是x
        row._2.apply(0).trim.split(",").zipWithIndex.map(y => new MatrixEntry(row._1, y._2, y._1.toDouble)) // 行id，列Id, value
      })
    })
    val yinstanceRdd = yxsplitedRdd.mapPartitions(par => {
      par.flatMap(row => { // 第一个inputs是y, 第二个inputs是x
        row._2.apply(1).trim.split(",").zipWithIndex.map(y => new MatrixEntry(row._1, y._2, y._1.toDouble)) // 行id，列Id, value
      })
    })
    val Y = new CoordinateMatrix(xinstanceRdd)
    val X = new CoordinateMatrix(yinstanceRdd)
    (Y, X)
  }

  /**
    * 矩阵加一个数， a + X
    *
    * @param X
    * @param a
    * @return
    */
  def addMatrix(X: CoordinateMatrix, a:Double): CoordinateMatrix = {
    val abc = X.entries.sparkContext.broadcast(a)
    new CoordinateMatrix(X.entries.map(row=> new MatrixEntry(row.i,row.j,row.value + abc.value)))
  }


}

/**
  * pls模型类
 *
  * @param sampleCount
  * @param XTrainColumnCount
  * @param YTrainColumnCount
  * @param componentCount
  * @param uid
  */
class PartialLeastSquaresRegressionModel (val sampleCount:Long,
                                          val trainDataCount:Long,
                                          val XTrainColumnCount:Int,
                                          val YTrainColumnCount:Int,
                                          val componentCount:Int,
                                          val XNormalizerModelColumnMean:Array[Double],
                                          val XNormalizerModelColumnStd:Array[Double],
                                          val YNormalizerModelColumnMean:Array[Double],
                                          val YNormalizerModelColumnStd:Array[Double],
                                          override val uid: String)
  extends RegressionModel[org.apache.spark.ml.linalg.DenseVector, PartialLeastSquaresRegressionModel] with Serializable {

  /** nipals
    * 1）转换权重：W（X-weights），C（Y-wights）
    * 2）因子得分：T（X-factor scores），U（Y-factor scores）
    * 3）载荷：P（X-loadings），Q（Y-loadings）
    */
  @transient var xScore: CoordinateMatrix = null //大小(sampleCount, componentCount)
  @transient var yScore: CoordinateMatrix = null //大小(sampleCount, componentCount)
  @transient var xWeight: CoordinateMatrix = null //大小(XColumnCount, componentCount)
  @transient var yWeight: CoordinateMatrix = null //大小(YColumnCount, componentCount)
  @transient var xLoading: CoordinateMatrix = null //大小(XColumnCount, componentCount)
  @transient var yLoading: CoordinateMatrix = null //大小(YColumnCount, componentCount)
  @transient var xRotation: CoordinateMatrix = null //大小(XColumnCount, componentCount)
  @transient var yRotation: CoordinateMatrix = null //大小(YColumnCount, componentCount) // 值默认是1
  @transient var coefficient: CoordinateMatrix = null //大小(XColumnCount, YColumnCount)
  override val numFeatures: Int = XTrainColumnCount
  var coefficientDenseMatrix: org.apache.spark.ml.linalg.DenseMatrix = null //大小(XColumnCount, YColumnCount)
  var trainingSummary: PartialLeastSquaresRegressionTrainingSummary = null

  override def copy(extra: ParamMap): PartialLeastSquaresRegressionModel = {
    val newModel = copyValues(new PartialLeastSquaresRegressionModel(sampleCount, trainDataCount, XTrainColumnCount,
      YTrainColumnCount, componentCount, XNormalizerModelColumnMean, XNormalizerModelColumnStd, YNormalizerModelColumnMean,
      YNormalizerModelColumnStd, uid
    ), extra)
    newModel
  }

  override def predict(features: org.apache.spark.ml.linalg.DenseVector): Double = {
    throw new RuntimeException("偏最小二乘不支持本方法，请调用方法 predictLabelVector")
  }

  /**
    * 把系数转换为 DenseMatrix, 系数矩阵大小(XColumnCount, YColumnCount)
    */
  def setCoefficientDenseMatrix(): Unit = {
    val values = coefficient.toRowMatrix().rows.collect().flatMap(_ .toArray) // 不会很大
    coefficientDenseMatrix = new org.apache.spark.ml.linalg.DenseMatrix(XTrainColumnCount, YTrainColumnCount, values, true)
  }

  /**
    * 预测的结果是一个向量，而不是一个数
    *
    * @param features      用来预测的行向量x
    * @param hasNormalized 是否已经标准化
    * @return 计算结果是一个行向量，大小等于features的大小
    */
  def predictLabelVector(features: org.apache.spark.ml.linalg.DenseVector, hasNormalized: Boolean):
  org.apache.spark.ml.linalg.DenseVector = {
    //  features * coefficient
    var normalizedVec = features
    if (!hasNormalized) {
      val normalizedData = normalizePredictData(features.values)
      normalizedVec = new org.apache.spark.ml.linalg.DenseVector(normalizedData)
    }
    // 不一次全部相乘，而是逐列相乘，预测数据依次与系数矩阵的每一列分别相乘求和
    val colCount = coefficientDenseMatrix.numCols
    val res = Array.fill(colCount)(0.0)
    for (i <- 0 until (colCount)) {
      val columnArray = getCoefficientDenseMatrixColumn(i)
      var sum = 0.0
      for (j <- 0 until (columnArray.length)) {
        sum += normalizedVec.apply(j) * columnArray.apply(j)
      }
      res.update(i, sum)
    }
    val inverData = addYMean(res)
    new org.apache.spark.ml.linalg.DenseVector(inverData)
  }

  /**
    * 得到系数矩阵的第几列
    *
    * @param columnIndex
    * @return
    */
  def getCoefficientColumn(columnIndex: Int): Array[Double] = {
    val columnIndexBc = coefficient.entries.sparkContext.broadcast(columnIndex) //大小等于Y的列数，不会很大
    coefficient.entries.filter(ent => ent.j == columnIndexBc.value).map(ent => ent.value).collect()
  }

  /**
    * 得到系数矩阵的第几列
    *
    * @param columnIndex
    * @return
    */
  def getCoefficientDenseMatrixColumn(columnIndex: Int): Array[Double] = {
    val rowCount = coefficientDenseMatrix.numRows
    val res = new Array[Double](rowCount) //大小等于Y的列数，一列不会很大
    for (i <- 0 until (rowCount)) {
      res.update(i, coefficientDenseMatrix.apply(i, columnIndex))
    }
    res
  }

  /**
    * 正则化
    * 减均值，除以 方差
    *
    * @param predictData 一行数据
    * @return
    */
  def normalizePredictData(predictData: Array[Double]): Array[Double] = {
    val res = Arrays.copyOf(predictData, predictData.length)
    for (i <- 0 until (XNormalizerModelColumnMean.length)) {
      var xstd = XNormalizerModelColumnStd.apply(i)
      if (xstd == 0.0) {
        xstd = 1.0
      }
      val tmp = (res.apply(i) - XNormalizerModelColumnMean.apply(i)) / xstd
      res.update(i, tmp)
    }
    res
  }

  /**
    * 逆正则化，把数据还原到之前大小
    * 乘方差，加均值
    *
    * @param y
    * @return
    */
  def addYMean(y: Array[Double]): Array[Double] = {
    val res = Arrays.copyOf(y, y.length)
    for (i <- 0 until (YNormalizerModelColumnMean.length)) {
      res.update(i, res.apply(i) + YNormalizerModelColumnMean.apply(i))
    }
    res
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    throw new RuntimeException("不使用该方法")
  }

}

/**
  * 模型度量
  * @param mse
  * @param rmse
  * @param r2
  * @param mae
  * @param explainedVariance
  */
case class PartialLeastSquaresRegressionTrainingSummary(mse:String,rmse:String, r2:String,
                                                 mae:String, explainedVariance:String) extends Serializable

