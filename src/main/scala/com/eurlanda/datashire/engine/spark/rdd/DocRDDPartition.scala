package com.eurlanda.datashire.engine.spark.rdd

import org.apache.spark.Partition

/**
 * Partition factory
 * Created by Juntao.Zhang on 2014/6/28.
 */
//ftp 共享文件夹rdd
private[spark] class FtpSharedFolderRDDPartition
(idx: Int, val ip: String, val port: Int, val path: String, val username: String, val password: String)
        extends DocRDDPartition(idx) {
}

//hdfs rdd
private[spark] class HdfsRDDPartition(idx: Int, val path: String) extends Partition {
    override def index = idx
}

//doc rdd 父类
class DocRDDPartition(idx: Int) extends Partition {
    override def index = idx
}
