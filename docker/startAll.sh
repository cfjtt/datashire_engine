#! /bin/bash

BASE_DIR=$(cd `dirname $0`; pwd)
cd $BASE_DIR
echo "------------------$(date)-----------------------" >> /var/log/ds_install.log

function exitWithError() {
  echo "异常退出:$1"
  echo "异常退出:$1" >> /var/log/ds_install.log
  exit 1
}

function exitIfError() {
  if [[ $? != 0 ]]; then
    echo "异常退出:$1"
    echo "异常退出:$1" >> /var/log/ds_install.log
    exit 1
  fi
}

function logMsg() {
  echo $1
  echo $1 >> /var/log/ds_install.log
}

sh /usr/local/datashire/init/init.sh
exitIfError "初始化失败, 退出"

if [ ! -f "init" ]; then

    /usr/local/datashire/kafka/bin/kafka-server-stop.sh
    /usr/local/datashire/kafka/bin/kafka-server-start.sh -daemon /usr/local/datashire/kafka/config/server.properties
    if [ $? -eq 0 ]; then
        logMsg "启动kafka成功"
        # 判断队列是否存在
        topic_exit=$(/usr/local/datashire/kafka/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER_ADDRESS |grep -i "^ds_log$")

        if [ -n "$topic_exit" ]; then
            logMsg "topc ds_log 已经存在于kafka集群中"
        else
            # 创建队列
            sleep 10
            /usr/local/datashire/kafka/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER_ADDRESS --replication-factor 1 --partitions 1 --topic ds_log
        fi
    else
        logMsg "启动kafka 失败"
        exit 1
    fi

    /usr/local/datashire/server/run_service_mysql.sh
    if [ $? -eq 0 ]; then
      logMsg "启动server 成功"
    else
      logMsg "启动server 失败"
      exit 1
    fi


    /usr/local/datashire/engine/run.sh
    if [ $? -eq 0 ]; then
        logMsg "启动engine成功"
    else
        logMsg "启动engine 失败"
        exit 1
    fi

    logMsg "启动成功 "
    tailf  /var/log/ds_install.log
else
    logMsg "没有正确初始化,放弃启动"
    exit 1
fi