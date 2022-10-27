package com.eurlanda.datashire.engine.spark.mllib.classification

/**
  * Created by Administrator on 2017-08-21.
  */
object util {

  /**
    * 分类阈值，1)元素个数要等于训练数据特征的数量,2)元素全部 >=0 且最多只能有一个0
    * DecisionTreeClassifier, GBTClassifier, LogisticRegression, NaiveBayes, RandomForestClassifier
    * @param thresholdsCsn
    * @return
    */
  def parseClassificationThresholds(thresholdsCsn:String): Array[Double] = {
    require(thresholdsCsn != null, "分类阈值不能是null")
    val tmp = thresholdsCsn.trim.replaceAll("'", "")
    require(!tmp.equals(""), "分类阈值不能是空字符串")
    var thresholds: Array[Double] = null
    try {
      thresholds = tmp.split(",").map(_.toDouble)
    } catch {
      case e: Throwable => {
        throw new RuntimeException("分类阈值不是csn格式")
      }
    }
    require(thresholds.forall(_ >= 0.0), "分类阈值应全都是正数或0")
    require(thresholds.count(_ == 0.0) <= 1, "分类阈值最多只能有一个元素是0")
    thresholds
  }

  /**
    * 获取字符串中的第一组数字
    *
    * @param str
    * @return
    */
   def getFirstNumberGroup(str: String): String = {
     val nums = str.trim.split("\\D+")
     if (nums != null && nums.length > 0) {
       for (s <- nums if !s.trim.equals("")) {
         return s
       }
     }
     null
   }


}
