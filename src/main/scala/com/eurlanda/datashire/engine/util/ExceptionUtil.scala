package com.eurlanda.datashire.engine.util

object ExceptionUtil {

  def translateExceptionMessage(e:Throwable): RuntimeException = {
    val errorMessage = e.getMessage
    if (errorMessage.contains("empty String")) {
      return new RuntimeException("数据有null或空值", e)
    }else if (errorMessage.contains("The size of BLOB/TEXT data inserted in one transaction is greater than 10% of redo log size")) {
      throw new RuntimeException("数据大小超过日志文件的10%，请在mysql的配置文件my.cnf中增加变量innodb_log_file_size的值", e)
    } else if (errorMessage.contains("Unknown column") || errorMessage.contains("in 'field list'")) {
      val msg = "Unknown column 'num_Bins' in 'field list'".replaceAll("Unknown column ", "落地表不存在字段").replaceAll(" in 'field list'", "")
      throw new RuntimeException(msg, e)
    } else if(errorMessage.contains("Bernoulli naive Bayes requires 0 or 1 feature values but found")){
      throw new RuntimeException("模型类型是Bernoulli时要求特征值是0或1",e)
    }else if(errorMessage.contains("Value at index 0 is null")){
      throw new RuntimeException("没有训练数据或测试数据，可能原因：总体或同一个key的数据过少或训练集比例过小或过大",e)
    }else if (errorMessage.contains("key not found")) {
      throw new RuntimeException("没有分配到足够的资源导致任务丢失，请稍后重试", e)
    }else if(errorMessage.contains("requires nonnegative feature values but found")){
      throw new RuntimeException("特征值要是非负数", e)
    }else if(errorMessage.contains("non-matching numClasses and thresholds.length")){
      throw new RuntimeException("分类标签不是从0开始且连续或阈值数量不等于标签数量", e)
    } else if(errorMessage.contains("empty collection")){
      throw new RuntimeException("没有训练数据或测试数据，可能原因：总体或同一个key的数据过少或训练集比例过小或过大", e)
    } else if(errorMessage.contains("Input validation failed")){
      throw new RuntimeException("标签不是0,1",e)
    }
    return null
  }

}
