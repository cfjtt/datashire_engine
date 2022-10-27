package com.eurlanda.datashire.engine.spark

import java.sql.SQLException
import java.util.{List => JList}

import com.eurlanda.datashire.engine.entity.TDataSource
import com.eurlanda.datashire.engine.mr.phoenix.PhoenixInputSplit
import com.eurlanda.datashire.engine.util.ConfigurationUtil
import com.google.common.base.{Preconditions, Throwables}
import com.google.common.collect.Lists
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.mapred.InputSplit
import org.apache.phoenix.iterate._
import org.apache.phoenix.jdbc.PhoenixResultSet
import org.apache.phoenix.monitoring.MetricType._
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * Created by zhudebin on 15-1-7.
 */
class PhoenixExtractRDD[T: ClassTag] (
  sc: SparkContext,
//  queryPlan: QueryPlan,
  dbi: TDataSource, selectSql:String,
  mapRow: (java.sql.ResultSet) => T,
  params: java.util.List[Object],
  defaultPartitions:Int = ConfigurationUtil.HBASE_EXTRACT_PARTITIONS_DEFAULT
  ) extends RDD[T](sc, Nil) {

  @DeveloperApi
  override def compute(theSplit: Partition, context: TaskContext): Iterator[T] = {

    var resultIterator: ResultIterator = null
    var resultSet: PhoenixResultSet = null
    val iter = new NextIterator[T] {
      import com.eurlanda.datashire.engine.spark.DatabaseUtils.{getConnection, getQueryPlan}
      val queryPlan = getQueryPlan(getConnection(dbi), selectSql, params)
      val partition = theSplit.asInstanceOf[PhoenixRDDPartition]
      val scans = partition.inputSplit.value.asInstanceOf[PhoenixInputSplit].getScans
      try {
        val iterators:java.util.List[PeekingResultIterator] = Lists.newArrayListWithExpectedSize(scans.size)
        val tableName:String = queryPlan.getTableRef.getTable.getPhysicalName.getString
        val renewScannerLeaseThreshold: Long = queryPlan.getContext.getConnection.getQueryServices.getRenewLeaseThresholdMilliSeconds
        for(scan <- scans) {
          val tableResultIterator = new TableResultIterator(
            queryPlan.getContext.getConnection.getMutationState(),scan,
            queryPlan.getContext.getReadMetricsQueue.allotMetric(SCAN_BYTES, tableName),
            renewScannerLeaseThreshold,queryPlan, MapReduceParallelScanGrouper.getInstance)
          val peekingResultIterator = LookAheadResultIterator.wrap(tableResultIterator)
          iterators.add(peekingResultIterator)
        }
        var iterator: ResultIterator = ConcatResultIterator.newIterator(iterators)
        if (queryPlan.getContext.getSequenceManager.getSequenceCount > 0) {
          iterator = new SequenceResultIterator(iterator, queryPlan.getContext.getSequenceManager)
        }
        resultIterator = iterator
        resultSet = new PhoenixResultSet(resultIterator, queryPlan.getProjector.cloneIfNecessary, queryPlan.getContext);
      } catch {
        case e: SQLException => {
          logError(String.format(" Error [%s] initializing PhoenixRecordReader. ", e.getMessage))
          Throwables.propagate(e)
        }
      }
//      logInfo("Input split: " + split.inputSplit)

      override protected def getNext(): T = {
        if(resultSet.next()) {
          mapRow(resultSet)
        } else {
          finished = true
          null.asInstanceOf[T]
        }
      }

      override protected def close(): Unit ={
        if (resultIterator != null) {
          try {
            resultIterator.close
          }
          catch {
            case e: SQLException => {
              logError(" Error closing resultset.")
              Throwables.propagate(e)
            }
          }
        }
      }
    }
    new InterruptibleIterator[T](context, iter)
  }

  override protected def getPartitions: Array[Partition] = {
    import com.eurlanda.datashire.engine.spark.DatabaseUtils.{getConnection, getQueryPlan}
    logInfo("======== hbase 抽取执行SQL ========" + selectSql)
    val queryPlan = getQueryPlan(getConnection(dbi), selectSql, params)
    val allSplits = queryPlan.getSplits
    Preconditions.checkNotNull(queryPlan)
    if(allSplits == null || allSplits.size()==0) {
      new Array[Partition](0)
    } else {
      Preconditions.checkNotNull(allSplits)
      val scans = queryPlan.getScans
      //    val array = new Array[Partition](scans.size())

      /**
      val f:ObjectOutputStream = new ObjectOutputStream(new FileOutputStream("/Users/zhudebin/Documents/iworkspace/datashire_svn/datashire_engine/branches/branch_phoenix_extract/log/scans"))
    f.writeObject(scans)
    f.flush()
    f.close()
        */
      val defaultParallelism = defaultPartitions
      val partitions = if (defaultParallelism < scans.size()) {
        scans.size()
      } else {
        if (defaultParallelism > allSplits.size) allSplits.size() else defaultParallelism
      }
      logInfo("====== hbase 抽取 分区数为：" + partitions + " ========== allSplits " + allSplits.size() + " =========")
      val scanNum = Math.ceil(allSplits.size().toDouble / partitions).toInt

      val array = new Array[Partition](partitions)
      /*for (i <- 0 until scans.size()) {
      loginfo(scans.get(i).get(0))
      array(i) = new PhoenixRDDPartition(i, EScan.toBytesList(scans.get(i)))
    }*/
      val allScan: JList[Scan] = new java.util.ArrayList[Scan]()
      for (ss <- scans) {
        for (s <- ss) {
          allScan.add(s)
        }
      }
      var index = 0
      for (i <- 0 until partitions) {
        array(i) = {
          val arr = if (allScan.size() - index < scanNum) {
            new java.util.ArrayList[Scan](allScan.size() - index)
          } else {
            new java.util.ArrayList[Scan](scanNum)
          }
          var arrIndex = 0
          while (index < allScan.size() && arrIndex < scanNum) {
            arr.add(arrIndex, allScan.get(index))
//            arr(arrIndex) = allScan.get(index)
            arrIndex += 1
            index += 1
          }

          new PhoenixRDDPartition(i,i, new PhoenixInputSplit(arr))
        }
      }
      array
    }
  }

}

class PhoenixRDDPartition(rddId: Int, idx: Int, @transient s: InputSplit) extends Partition {
  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx
}
