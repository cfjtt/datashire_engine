#! /bin/bash
cd $ds_path/engine

set -e
function exitWithError() {
  echo "异常退出:$1" >> $ds_path/log/ds_install.log
  exit 1
}
function exitIfError() {
  if [[ $? != 0 ]]; then
    echo "异常退出:$1" >> $ds_path/log/ds_install.log
    exit 1
  fi
}

if [ -d engine_jar ]; then
  rm -Rf engine_jar
fi

mkdir engine_jar
cp engine.jar engine_jar/
cd engine_jar

jar xf engine.jar

#修改配置文件
set +e
if [[ -z $HDFS_SITE ]]; then
  source $ds_path/init/initHadoopConfig.sh
  exitIfError "初始化engine hadoop 配置异常"
  export HDFS_SITE="$(cat $ds_path/init/hadoop_conf/hdfs-site.xml)"
  export YARN_SITE="$(cat $ds_path/init/hadoop_conf/yarn-site.xml)"
  export CORE_SITE="$(cat $ds_path/init/hadoop_conf/core-site.xml)"
fi
source $ds_path/init/initEngine_conf.sh
exitIfError "初始化engine 参数异常"
set -e

# 将配置文件夹整体更新到jar
jar uf engine.jar conf

# 覆盖engine.jar

cd ..
echo $(pwd)
rm -f engine.jar
cp engine_jar/engine.jar ./
#rm -Rf engine_jar

# export SPARK_LOCAL_IP= 引擎所属集群的ip

# 生成 hdfs-site.xml,yarn-site.xml
echo $HDFS_SITE > hadoop_conf/hdfs-site.xml
echo $YARN_SITE > hadoop_conf/yarn-site.xml
echo $CORE_SITE > hadoop_conf/core-site.xml