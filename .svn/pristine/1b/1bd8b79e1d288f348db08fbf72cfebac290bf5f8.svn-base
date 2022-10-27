#! /bin/bash

BASE_DIR=$(cd `dirname $0`; pwd)
cd $BASE_DIR

export ds_path=/usr/local/datashire

mkdir $ds_path/log

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

# 执行第一次运行的时候的初始化
# 1 检查 init.sh 文件是否存在
if [ -f "init" ]; then

    # 2 存在执行初始化脚本
    echo "开始执行初始化" >> $ds_path/log/ds_install.log
    # 修改三个组件的配置参数
    # kafka
    chmod a+x $ds_path/init/initKafka.sh
    sh $ds_path/init/initKafka.sh
    exitIfError "初始化kafka失败"

    # server
    chmod a+x $ds_path/init/initServer.sh
    sh $ds_path/init/initServer.sh
    exitIfError "初始化server失败"

    # engine
    chmod a+x $ds_path/init/initEngine.sh
    sh $ds_path/init/initEngine.sh
    exitIfError "初始化engine失败"

    # 初始上传jar 到hdfs
    chmod a+x $ds_path/engine/initYarnJar.sh
    sh $ds_path/engine/initYarnJar.sh $ds_path/engine/spark-yarn-libs
    exitIfError "初始化上传engine 依赖jar失败"

    # 初始化数据库
    chmod a+x $ds_path/init/initdb/initdb.sh
    sh $ds_path/init/initdb/initdb.sh
    exitIfError "初始化数据库失败"

    rm -f init
    echo "初始化成功" >> $ds_path/log/ds_install.log
  else
    # 3 不存在,啥都不用做,跳过
    echo "已经初始化了" >> $ds_path/log/ds_install.log
fi