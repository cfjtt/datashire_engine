package org.elasticsearch.spark

import java.util.{Map => JMap}

import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.rest.{InitializationUtils, RestService}
import org.elasticsearch.hadoop.serialization.builder.ValueWriter
import org.elasticsearch.hadoop.serialization.field.FieldExtractor
import org.elasticsearch.hadoop.serialization.{BytesConverter, JdkBytesConverter}
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.spark.serialization.{ScalaMapFieldExtractor, ScalaMetadataExtractor, ScalaValueWriter}

import scala.collection.Map
import scala.reflect.ClassTag

/**
  * Created by zhudebin on 16/8/8.
  */
object ESSparkUtil {

  def saveToEs(rdd: RDD[_], resource: String, cfg: Map[String, String]) {
    saveToEs(rdd, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(rdd: RDD[_], cfg: Map[String, String]) {
    doSaveToEs(rdd, cfg, false)
  }

  private def doSaveToEs(rdd: RDD[_], cfg: Map[String, String], hasMeta: Boolean) {
//    CompatUtils.warnSchemaRDD(rdd, LogFactory.getLog("org.elasticsearch.spark.rdd.EsSpark"))

    if (rdd == null || rdd.partitions.length == 0) {
      return
    }

    val sparkCfg = new SparkSettingsManager().load(rdd.sparkContext.getConf)
    val config = new PropertiesSettings().load(sparkCfg.save())
    import scala.collection.JavaConverters.mapAsJavaMapConverter
    config.merge(cfg.asJava)

    InitializationUtils.checkIdForOperation(config);
    InitializationUtils.checkIndexExistence(config, null);

    val configStr = config.save()

    rdd.sparkContext.runJob(rdd, (context: TaskContext, iter: Iterator[_]) => {
      new DsEsRDDWriter(configStr, hasMeta).write(context, iter)
    })
  }
}

private[spark] class DsEsRDDWriter[T: ClassTag](val serializedSettings: String,
                                              val runtimeMetadata: Boolean = false)
  extends Serializable {

  protected val log = LogFactory.getLog(this.getClass())

  lazy val settings = {
    val settings = new PropertiesSettings().load(serializedSettings);

    InitializationUtils.setValueWriterIfNotSet(settings, valueWriter, log)
    InitializationUtils.setBytesConverterIfNeeded(settings, bytesConverter, log)
    InitializationUtils.setFieldExtractorIfNotSet(settings, fieldExtractor, log)

    settings
  }

  lazy val metaExtractor = new ScalaMetadataExtractor()

  def write(taskContext: TaskContext, data: Iterator[T]) {
    val writer = RestService.createWriter(settings, taskContext.partitionId, -1, log)

    taskContext.addTaskCompletionListener((TaskContext) => writer.close())

    if (runtimeMetadata) {
      writer.repository.addRuntimeFieldExtractor(metaExtractor)
    }

    while (data.hasNext) {
      writer.repository.writeToIndex(processData(data))
    }
  }

  protected def valueWriter: Class[_ <: ValueWriter[_]] = classOf[ScalaValueWriter]
  protected def bytesConverter: Class[_ <: BytesConverter] = classOf[JdkBytesConverter]
  protected def fieldExtractor: Class[_ <: FieldExtractor] = classOf[ScalaMapFieldExtractor]

  protected def processData(data: Iterator[T]): Any = {
    val next = data.next
    if (runtimeMetadata) {
      //TODO: is there a better way to do this cast
      next match {
        case (k, v) =>
        {
          // use the key to extract metadata
          metaExtractor.setObject(k);
          // return the value to be used as the document
          v
        }
      }
    } else {
      next
    }
  }
}
