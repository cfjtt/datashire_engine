package org.apache.spark

import java.io.IOException
import java.text.NumberFormat
import java.util.Date

import com.eurlanda.datashire.engine.util.DateUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred._
import org.apache.spark.internal.Logging
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.util.SerializableJobConf

/**
  * Internal helper class that saves an RDD using a Hadoop OutputFormat.
  *
  * Saves the RDD using a JobConf, which should contain an output key class, an output value class,
  * a filename to write to, etc, exactly like in a Hadoop MapReduce job.
  */
class CustomSparkHadoopWriter(@transient jobConf: JobConf)
  extends Logging with Serializable {

  private val now = new Date()
  private val conf = new SerializableJobConf(jobConf)

  private var jobID = 0
  private var splitID = 0
  private var attemptID = 0
  private var jID: SerializableWritable[JobID] = null
  private var taID: SerializableWritable[TaskAttemptID] = null

  @transient private var writer: RecordWriter[AnyRef, AnyRef] = null
  @transient private var format: OutputFormat[AnyRef, AnyRef] = null
  @transient private var committer: OutputCommitter = null
  @transient private var jobContext: JobContext = null
  @transient private var taskContext: TaskAttemptContext = null

  def preSetup() {
    setIDs(0, 0, 0)
    HadoopRDD.addLocalConfiguration("", 0, 0, 0, conf.value)

    val jCtxt = getJobContext()
    getOutputCommitter().setupJob(jCtxt)
  }


  def setup(jobid: Int, splitid: Int, attemptid: Int) {
    setIDs(jobid, splitid, attemptid)
    HadoopRDD.addLocalConfiguration(DateUtil.format("yyyyMMddHHmm", now),
      jobid, splitID, attemptID, conf.value)
  }

  def open() {
    val numfmt = NumberFormat.getInstance()
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)

    val outputName = "part-" + DateUtil.format("yyyyMMddHHmmss", now) + "-"  + numfmt.format(splitID)
    val path = FileOutputFormat.getOutputPath(conf.value)
    val fs: FileSystem = {
      if (path != null) {
        path.getFileSystem(conf.value)
      } else {
        FileSystem.get(conf.value)
      }
    }

    getOutputCommitter().setupTask(getTaskContext())
    writer = getOutputFormat().getRecordWriter(fs, conf.value, outputName, Reporter.NULL)
  }

  def write(key: AnyRef, value: AnyRef) {
    if (writer != null) {
      writer.write(key, value)
    } else {
      throw new IOException("Writer is null, open() has not been called")
    }
  }

  def close() {
    writer.close(Reporter.NULL)
  }

  def commit() {
    SparkHadoopMapRedUtil.commitTask(getOutputCommitter(), getTaskContext(), jobID, splitID)
  }

  def commitJob() {
    val cmtr = getOutputCommitter()
    cmtr.commitJob(getJobContext())
  }

  // ********* Private Functions *********

  private def getOutputFormat(): OutputFormat[AnyRef, AnyRef] = {
    if (format == null) {
      format = conf.value.getOutputFormat()
        .asInstanceOf[OutputFormat[AnyRef, AnyRef]]
    }
    format
  }

  private def getOutputCommitter(): OutputCommitter = {
    if (committer == null) {
      committer = conf.value.getOutputCommitter
    }
    committer
  }

  private def getJobContext(): JobContext = {
    if (jobContext == null) {
      jobContext = new JobContextImpl(conf.value, jID.value)
    }
    jobContext
  }

  private def getTaskContext(): TaskAttemptContext = {
    if (taskContext == null) {
      taskContext = newTaskAttemptContext(conf.value, taID.value)
    }
    taskContext
  }

  protected def newTaskAttemptContext(
                                       conf: JobConf,
                                       attemptId: TaskAttemptID): TaskAttemptContext = {
    new TaskAttemptContextImpl(conf, attemptId)
  }

  private def setIDs(jobid: Int, splitid: Int, attemptid: Int) {
    jobID = jobid
    splitID = splitid
    attemptID = attemptid

    jID = new SerializableWritable[JobID](SparkHadoopWriter.createJobID(now, jobid))
    taID = new SerializableWritable[TaskAttemptID](
      new TaskAttemptID(new TaskID(jID.value, true, splitID), attemptID))
  }
}

object CustomSparkHadoopWriter {
  def createJobID(time: Date, id: Int): JobID = {
    val jobtrackerID = DateUtil.format("yyyyMMddHHmm", time)
    new JobID(jobtrackerID, id)
  }

  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (outputPath == null || fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath.makeQualified(fs)
  }
}
