package com.eurlanda.datashire.engine.spark.util

import java.util
import java.util.{List => JList}

import org.ansj.domain.Term
import org.ansj.library.NatureLibrary
import org.ansj.splitWord.analysis.ToAnalysis

import scala.collection.JavaConverters._
import scala.collection.immutable.List

/**
 * Created by zhudebin on 14-6-4.
 */
class SegProcessor {


}

object SegProcessor {

    var isLoadDic = false
    var emotionDic:List[String] = _

    def loadDic(list: List[String]) {
      // todo 自定义分词
//        list.foreach((key:String) => UserDefineLibrary.insertWord(key.toLowerCase, "userDefine", 1000))
        emotionDic = list
        isLoadDic = true;
    }

    /**
     * 对str分词
     * @param str
     * @param list
     * @return
     */
    def getWords(str: String, list: List[String]):List[String] = {
        if(!isLoadDic) {
            loadDic(list)
        }

        ToAnalysis.parse(str).asScala.map((f:Term) => {
            f.getName
        }).toList
    }

    /**
     * 对str分词, java ban
     * @param str
     * @param list
     * @return
     */
    def getWords(str: String, list: java.util.List[String]):java.util.List[String] = {
        new util.ArrayList[String](getWords(str, list.asScala.toList).asJavaCollection)
    }

    /**
     * 由字符串取得特征值
     *
     * @param str 输入的字符串。
     *
     * @return 返回特征值数组。
     */
    def getFeature(str: String, dict: java.util.List[String]):String = {
        if(!isLoadDic) {
            loadDic(dict.asScala.toList)
        }
        getFeatureByAnsj(str).mkString(",")
    }

    /**
     * 由字符串取得特征值
     * ansj
     * @param str 输入的字符串。
     *
     * @return 返回特征值数组。
     */
    def getFeatureByAnsj(str: String):Array[Double] = {
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
