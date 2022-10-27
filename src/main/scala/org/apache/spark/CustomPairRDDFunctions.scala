package org.apache.spark

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.OutputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.util.{SerializableConfiguration, Utils}

import scala.reflect.ClassTag
import scala.util.DynamicVariable

/**
  * Created by zhudebin on 16/5/11.
  */
class CustomPairRDDFunctions[K, V](self: RDD[(K, V)])
                                  (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging
    with Serializable
{

  /**
    * Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for
    * that storage system. The JobConf should set an OutputFormat and any output paths required
    * (e.g. a table name to write to) in the same way as it would be configured for a Hadoop
    * MapReduce job.
    */
  def saveAsHadoopDataset(conf: JobConf): Unit = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val wrappedConf = new SerializableConfiguration(hadoopConf)
    val outputFormatInstance = hadoopConf.getOutputFormat
    val keyClass = hadoopConf.getOutputKeyClass
    val valueClass = hadoopConf.getOutputValueClass
    if (outputFormatInstance == null) {
      throw new SparkException("Output format class not set")
    }
    if (keyClass == null) {
      throw new SparkException("Output key class not set")
    }
    if (valueClass == null) {
      throw new SparkException("Output value class not set")
    }
    SparkHadoopUtil.get.addCredentials(hadoopConf)

    logDebug("Saving as hadoop file of type (" + keyClass.getSimpleName + ", " +
      valueClass.getSimpleName + ")")

    if (isOutputSpecValidationEnabled) {
      // FileOutputFormat ignores the filesystem parameter
      val ignoredFs = FileSystem.get(hadoopConf)
      hadoopConf.getOutputFormat.checkOutputSpecs(ignoredFs, hadoopConf)
    }

    val writer = new CustomSparkHadoopWriter(hadoopConf)
    writer.preSetup()

    val writeToFile = (context: TaskContext, iter: Iterator[(K, V)]) => {
      // 该分片不存在数据则不创建文件
      if(iter.nonEmpty) {
        val config = wrappedConf.value
        // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
        // around by taking a mod. We expect that no task will be attempted 2 billion times.
        val taskAttemptId = (context.taskAttemptId % Int.MaxValue).toInt

        val outputMetricsAndBytesWrittenCallback: Option[(OutputMetrics, () => Long)] =
          initHadoopOutputMetrics(context)
//        val (outputMetrics, bytesWrittenCallback) = initHadoopOutputMetrics(context)

        writer.setup(context.stageId, context.partitionId, taskAttemptId)
        writer.open()
        var recordsWritten = 0L

        Utils.tryWithSafeFinally {
          while (iter.hasNext) {
            val record = iter.next()
            writer.write(record._1.asInstanceOf[AnyRef], record._2.asInstanceOf[AnyRef])

            // Update bytes written metric every few records
            maybeUpdateOutputMetrics(outputMetricsAndBytesWrittenCallback, recordsWritten)
            recordsWritten += 1
          }
        } {
          writer.close()
        }
        writer.commit()
        outputMetricsAndBytesWrittenCallback.foreach { case (om, callback) =>
          om.setBytesWritten(callback())
          om.setRecordsWritten(recordsWritten)
        }
//        bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
//        outputMetrics.setRecordsWritten(recordsWritten)
      } else {
//        writer.close()
      }

    }

    self.context.runJob(self, writeToFile)
    writer.commitJob()
  }

  // return type: (output metrics, bytes written callback), defined only if the latter is defined
  private def initHadoopOutputMetrics(context: TaskContext): Option[(OutputMetrics, () => Long)] = {
    val bytesWrittenCallback = SparkHadoopUtil.get.getFSBytesWrittenOnThreadCallback()
    bytesWrittenCallback.map { b =>
      (context.taskMetrics().outputMetrics, b)
    }
  }

  private def maybeUpdateOutputMetrics(outputMetricsAndBytesWrittenCallback: Option[(OutputMetrics, () => Long)],
                                        recordsWritten: Long): Unit = {
    if (recordsWritten % PairRDDFunctions.RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES == 0) {
      outputMetricsAndBytesWrittenCallback.foreach { case (om, callback) =>
        om.setBytesWritten(callback())
        om.setRecordsWritten(recordsWritten)
      }
    }
  }

  // Note: this needs to be a function instead of a 'val' so that the disableOutputSpecValidation
  // setting can take effect:
  private def isOutputSpecValidationEnabled: Boolean = {
    val validationDisabled = PairRDDFunctions.disableOutputSpecValidation.value
    val enabledInConf = self.conf.getBoolean("spark.hadoop.validateOutputSpecs", true)
    enabledInConf && !validationDisabled
  }
}

private[spark] object CustomPairRDDFunctions {
  val RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES = 256

  /**
    * Allows for the `spark.hadoop.validateOutputSpecs` checks to be disabled on a case-by-case
    * basis; see SPARK-4835 for more details.
    */
  val disableOutputSpecValidation: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)
}
