package com.eurlanda.datashire.engine.spark.mllib.associationrules

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable


/**
  * Created by hwj on 2016-05-11
  *
  */
class AssociationRulesSquid extends Serializable {

  var minSupport = 0.0 //最小支持度
  var minConfidence = 0.0 //最小可信度
  private val log: Log = LogFactory.getLog(classOf[AssociationRulesSquid])
  val splitFlag = "," //

  def run(preRDD:RDD[scala.Array[String]],sparkSession: SparkSession):DataFrame= {

    require(minSupport>=0.0 && minSupport<=1.0,"关联规则模型最小支持度要在区间[0,1]内")
    require(minConfidence>=0.0 && minConfidence<=1.0,"关联规则模型最可信度支持度要在区间[0,1]内")

    if(preRDD == null){
      throw new RuntimeException("关联规则的训练数据是null")
    }
    val dataRDD = preRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val dataCount = dataRDD.count()
    if(dataCount==0){
      throw new RuntimeException("关联规则没有训练数据")
    }
    try {
      val fpgrowth = new FPGrowth().setMinSupport(minSupport)
      val freqItemsetsmodel = fpgrowth.run(dataRDD)
      val associationRulesmodel = freqItemsetsmodel.generateAssociationRules(minConfidence)
      val itemsAssociationRulesRDD = getItemsAssociationRulesDataFrame(freqItemsetsmodel, associationRulesmodel,
                                     dataCount, sparkSession)
      itemsAssociationRulesRDD
    }catch {
      case e:Throwable=> {
        log.error(e.getCause)
        val errorMessage = e.getMessage
        if (errorMessage.contains("key not found")) {
          throw new RuntimeException("没有分配到足够的资源导致任务丢失，请稍后重试", e)
        }
        throw e
      }
    }finally {
      dataRDD.unpersist()
    }
  }

  /**
    *
    * @param freqItemsetsmodel
    * @param associationRulesmodel
    * @param dataCount
    * @return
    */
  private def getItemsAssociationRulesRDD(freqItemsetsmodel:FPGrowthModel[String],
                                          associationRulesmodel:RDD[AssociationRules.Rule[String]],
                                          dataCount :Long) :RDD[mutable.HashMap[String, String]] = {
    val rule_size = associationRulesmodel.count()
    val freqItemsetstmp = associationRulesmodel.sparkContext.broadcast(freqItemsetsmodel.freqItemsets.collect())
    val total_datasettmp = associationRulesmodel.sparkContext.broadcast(dataCount)
    associationRulesmodel.map { x => {
      val itemsRuleMap = new mutable.HashMap[String, String]()
      // itemsRuleMap.put("ID", IDtmp.value.toString)
      itemsRuleMap.put("total_dataset", total_datasettmp.value.toString)
      itemsRuleMap.put("rule_size", rule_size.toString)
      itemsRuleMap.put("antecedent", x.antecedent.mkString(","))
      itemsRuleMap.put("consequent", x.consequent.mkString(","))
      val antecedent_instance_size = queryItemFreq(freqItemsetstmp.value, x.antecedent)
      itemsRuleMap.put("antecedent_instance_size", antecedent_instance_size.toString)
      itemsRuleMap.put("antecedent_size", x.antecedent.length.toString)
      val antecedent_Support = 1.0 * antecedent_instance_size / total_datasettmp.value
      itemsRuleMap.put("antecedent_support", antecedent_Support.toString)
      itemsRuleMap.put("consequent_size", x.consequent.length.toString)
      val consequent_instance_size = queryItemFreq(freqItemsetstmp.value, x.consequent)
      itemsRuleMap.put("consequent_instance_size", consequent_instance_size.toString)
      val consequent_Support = 1.0 * consequent_instance_size / total_datasettmp.value
      itemsRuleMap.put("consequent_support", consequent_Support.toString)
      val rule_support = 1.0 * queryItemFreq(freqItemsetstmp.value, (x.antecedent ++ x.consequent).distinct) / total_datasettmp.value
      itemsRuleMap.put("rule_support", rule_support.toString)
      itemsRuleMap.put("confidence", x.confidence.toString)
      itemsRuleMap.put("lift", (1.0 * x.confidence / consequent_Support).toString)
      itemsRuleMap.put("deployment_capability", (antecedent_Support - rule_support).toString)
      //ID += 1
      itemsRuleMap
    }
    }
  }

  /**
    *
    * @param freqItemsetsmodel
    * @param associationRulesmodel
    * @param dataCount
    * @return
    */
  private def getItemsAssociationRulesDataFrame(freqItemsetsmodel:FPGrowthModel[String],
                                          associationRulesmodel:RDD[AssociationRules.Rule[String]],
                                          dataCount :Long,sparkSession: SparkSession) :DataFrame = {
    val  rule_size = associationRulesmodel.count()
    val structType = getStructType
    if(rule_size == 0) {
     return sparkSession.createDataFrame(sparkSession.emptyDataFrame.rdd, structType)
    }
    val rule_sizeBc = associationRulesmodel.sparkContext.broadcast(rule_size)

    val freqItemsetstmp = associationRulesmodel.sparkContext.broadcast(freqItemsetsmodel.freqItemsets.collect())
    val dataCountBc = associationRulesmodel.sparkContext.broadcast(dataCount)

    val rowRdd = associationRulesmodel.map { x => {

      val antecedent_instance_size = queryItemFreq(freqItemsetstmp.value, x.antecedent)
      val antecedent_Support = 1.0 * antecedent_instance_size / dataCountBc.value
      val consequent_instance_size = queryItemFreq(freqItemsetstmp.value, x.consequent)
      val consequent_Support = 1.0 * consequent_instance_size / dataCountBc.value
      val rule_support = 1.0 * queryItemFreq(freqItemsetstmp.value, (x.antecedent ++ x.consequent).distinct) / dataCountBc.value
      Row(dataCountBc.value, // total_dataset
        rule_sizeBc.value, // rule_size
        x.antecedent.mkString(","), //antecedent
        x.consequent.mkString(","), //consequent
        x.antecedent.length, //antecedent_size 前项中元素个数
        antecedent_instance_size, // antecedent_instance_size  包含前项的数据记录数
        antecedent_Support, // antecedent_support
        x.consequent.length, //consequent_size 后项中元素个数
        consequent_instance_size, // consequent_instance_size 包含后项的数据记录数
        consequent_Support, // consequent_support
        rule_support, // rule_support
        x.confidence, //confidence
        1.0 * x.confidence / consequent_Support, //lift
        antecedent_Support - rule_support //deployment_capability
      )
    }
    }
    sparkSession.createDataFrame(rowRdd, structType)
  }

  /**
    * 查询包含项的次数
    */
  private def queryItemFreq(freqItemsetsmodel: Array[FPGrowth.FreqItemset[String]],items: Array[String]): Int = {
    freqItemsetsmodel.filter(x => isArrayElementsEqual(x.items, items)).head.freq.toInt
  }

  /**
    * 查询包含项的次数
    */
  private def queryItemFreq(freqItemsetsmodel: FPGrowthModel[String],items: Array[String]): Int = {
    freqItemsetsmodel.freqItemsets.filter(x => isArrayElementsEqual(x.items, items)).first().freq.toInt
  }

  /**
    * 设置数据类型
    *
    * @return
    */
  private def getStructType(): StructType = {
    val structType = new StructType()
     .add("total_dataset", DataTypes.LongType, false)
     .add("rule_size", DataTypes.LongType, false)
     .add("antecedent", DataTypes.StringType, false)
     .add("consequent", DataTypes.StringType, false)
     .add("antecedent_size", DataTypes.IntegerType, false)
     .add("antecedent_instance_size", DataTypes.IntegerType, false)
     .add("antecedent_support", DataTypes.DoubleType, false)
     .add("consequent_size", DataTypes.IntegerType, false)
     .add("consequent_instance_size", DataTypes.IntegerType, false)
     .add("consequent_support", DataTypes.DoubleType, false)
     .add("rule_support", DataTypes.DoubleType, false)
     .add("confidence", DataTypes.DoubleType, false)
     .add("lift", DataTypes.DoubleType, false)
     .add("deployment_capability", DataTypes.DoubleType, false)

    structType
  }


  /**
    * 获取模型版本
    *
    * @param dataFrame
    * @param modelVersion 选择的模型版本， -1 表示最新版本
    * @return
    */
  private def getModelVersion(dataFrame: DataFrame, modelVersion: Integer): Integer = {
    if (modelVersion == -1) {
      dataFrame.agg(Map("version" -> "max")).first().getInt(0)
    } else {
      modelVersion
    }
  }

  /**
    * arr1 是否等于或包含 arr2
    *
    * @param arr1
    * @param arr2
    * @return
    */
  private def isArrayElementsEqualOrContain(arr1: Array[String], arr2: Array[String]): Boolean = {
    if (arr1.length < arr2.length) {
      return false
    }
    for (emt2 <- arr2 if (!arr1.contains(emt2))) {
      return false
    }
    return true
  }

  /**
    * arr1的元素是否等于 arr2的元素
    *
    * @param arr1
    * @param arr2
    * @return
    */
  private def isArrayElementsEqual(arr1: Array[String], arr2: Array[String]): Boolean = {
    if (arr1.length != arr2.length) {
      return false
    }
    for (emt2 <- arr2 if (!arr1.contains(emt2))) {
      return false
    }
    return true
  }



}
