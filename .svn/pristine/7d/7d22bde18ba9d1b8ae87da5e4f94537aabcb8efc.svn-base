#! /bin/bash
export SPARK_EXECUTOR_MEMORY=2G
export SPARK_EXECUTOR_CORES=1
export SPARK_DRIVER_MEMORY=1G
export HDFS_CLUSTER=cluster
export ZOOKEEPER_ADDRESS=192.168.137.101:2181
export MYSQL_HOST=192.168.137.130
export MYSQL_PORT=3306
export MYSQL_USERNAME=root
export MYSQL_PASSWORD=111111
#使用具体的配置文件

export HDFS_SITE="$(cat /usr/local/datashire/hadoop_conf/hdfs-site.xml)"
export YARN_SITE="$(cat /usr/local/datashire/hadoop_conf/yarn-site.xml)"
export CORE_SITE="$(cat /usr/local/datashire/hadoop_conf/core-site.xml)"

#使用具体的配置参数
#export HADOOP_JOURNAL_URI="qjournal://journal01:8485;journal02:8485;journal03:8485"
#export HADOOP_NN_HOSTS=namenode01,namenode02
#export HADOOP_RM_HOSTS=resourcemanager01,resourcemanager02
#export HADOOP_ZK_URI=zookeeper01:2181,zookeeper02:2181,zookeeper03:2181
#export HDFS_HA_ENABLE=true
#export NAMESERVICES=cluster
#export YARN_HA_ENABLE=true
#动态设置为true, SPARK_EXECUTOR_INSTANCES就不需要设置

export SPARK_DYNAMICALLOCATION_ENABLED=false
export SPARK_EXECUTOR_INSTANCES=3
