package com.eurlanda.datashire.engine.spark

import java.util.{List => JList}

import org.ansj.domain.Term
import org.ansj.splitWord.analysis.ToAnalysis

import scala.collection.JavaConverters._

/**
 * Created by zhudebin on 14-3-25.
 */
object TFeaturesTransformationUtils {

  /**
   * 由字符串取得情感特征值
   *
   * @param str 输入的字符串。
   * @return 返回特征值数组。
   */
  def getEmotionFeature(str: String, emotionDic:java.util.ArrayList[String]) = {
      getEmotionFeatureByAnsj(str, emotionDic)
  }

    /**
     * 由字符串取得情感特征值
     *
     * @param str 输入的字符串。
     *
     * @return 返回特征值数组。
     */
    def getEmotionFeatureByAnsj(str: String, emotionDic:java.util.ArrayList[String]):Array[Double] = {
        val feature = new Array[Double](emotionDic.size)
        if (str != null && (!str.isEmpty())) {
            val terms:JList[Term] = ToAnalysis.parse(str).getTerms

            terms.asScala.foreach((p:Term) => {
                val pos = emotionDic.indexOf(p.getName)
                if(pos != -1) {
                    feature(pos) = feature(pos)+1d
                }
            })
        }
        feature
    }

}
