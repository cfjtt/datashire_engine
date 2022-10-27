#! /bin/bash

# 初始化环境变量,没有设置,采用默认值
hostname=$(hostname)

function exitWithError() {
  echo "异常退出:$1" >> $ds_path/log/ds_install.log
  exit 1
}

function init_env() {
    if [ ! -n "$SPARK_EXECUTOR_MEMORY" ]; then
      SPARK_EXECUTOR_MEMORY=2G
    fi

    if [ ! -n "$SPARK_EXECUTOR_CORES" ]; then
      SPARK_EXECUTOR_CORES=1
    fi

    if [ ! -n "$SPARK_DRIVER_MEMORY" ]; then
      SPARK_DRIVER_MEMORY=1G
    fi

    if [ ! -n "$HDFS_CLUSTER" ]; then
      exitWithError "请设置HDFS集群的地址";
    fi

    if [ ! -n "$ZOOKEEPER_ADDRESS" ]; then
      exitWithError "请设置ZOOKEEPER_ADDRESS"
    fi

    if [ ! -n "$MYSQL_HOST" ]; then
      exitWithError "请设置MYSQL_HOST"
    fi

    if [ ! -n "$MYSQL_PORT" ]; then
      exitWithError "请设置MYSQL_PORT"
    fi
    if [ ! -n "$MYSQL_USERNAME" ]; then
      exitWithError "请设置MYSQL_USERNAME"
    fi

    if [ ! -n "$MYSQL_PASSWORD" ]; then
      exitWithError "请设置MYSQL_PASSWORD"
    fi

    # SPARK_DYNAMICALLOCATION_ENABLED 是否为动态获取资源模式
    if [ ! -n "$SPARK_DYNAMICALLOCATION_ENABLED" ]; then
      exitWithError "请设置 SPARK_DYNAMICALLOCATION_ENABLED"
    esle
      if [ "$SPARK_DYNAMICALLOCATION_ENABLED" == "false" ]; then
        # 不是动态获取资源,需要制定启动的实例数
        if [ ! -n "$SPARK_EXECUTOR_INSTANCES" ]; then
          exitWithError "请设置 SPARK_EXECUTOR_INSTANCES, 设置启动的实例数"
        fi
      else
        export SPARK_EXECUTOR_INSTANCES=0
      fi
    fi

    if [ ! -n "$WEB_SERVICE_IP" ]; then
      WEB_SERVICE_IP=$hostname
    fi

    if [ ! -n "$MAIL_USERNAME" ]; then
      MAIL_PROTOCOL=smtp
      MAIL_HOST=smtp.exmail.qq.com
      MAIL_PORT=465
      MAIL_USERNAME=ceshi@eurlanda.com
      MAIL_PASSWORD=Ce123456
      MAIL_SMTP_AUTH=true
      MAIL_SMTP_STARTTLS_ENABLE=true
    fi

}

init_env

sed -i "s/^SPARK_EXECUTOR_MEMORY=.*/SPARK_EXECUTOR_MEMORY=$SPARK_EXECUTOR_MEMORY/" conf/configuration.properties
sed -i "s/^SPARK_EXECUTOR_CORES=.*/SPARK_EXECUTOR_CORES=$SPARK_EXECUTOR_CORES/" conf/configuration.properties
sed -i "s/^SPARK_EXECUTOR_INSTANCES=.*/SPARK_EXECUTOR_INSTANCES=$SPARK_EXECUTOR_INSTANCES/" conf/configuration.properties
sed -i "s/^SPARK_DYNAMICALLOCATION_ENABLED=.*/SPARK_DYNAMICALLOCATION_ENABLED=$SPARK_DYNAMICALLOCATION_ENABLED/" conf/configuration.properties
sed -i "s/^SPARK_DRIVER_MEMORY=.*/SPARK_DRIVER_MEMORY=$SPARK_DRIVER_MEMORY/" conf/configuration.properties
sed -i "s#^SPARK_YARN_ENGINE_JARS_DIR=.*#SPARK_YARN_ENGINE_JARS_DIR=hdfs://$HDFS_CLUSTER/user/datashire/spark201-ds-jars/#" conf/configuration.properties
sed -i "s/KAFKA_BROKER_LIST=.*/KAFKA_BROKER_LIST=$hostname:9092/" conf/configuration.properties
sed -i "s/^KAFKA_ZOOKEEPER_ADDRESS=.*/KAFKA_ZOOKEEPER_ADDRESS=$ZOOKEEPER_ADDRESS/" conf/configuration.properties
sed -i "s/^INNER_MYSQL_HOST=.*/INNER_MYSQL_HOST=$MYSQL_HOST/" conf/configuration.properties
sed -i "s/^INNER_MYSQL_PORT=.*/INNER_MYSQL_PORT=$MYSQL_PORT/" conf/configuration.properties
sed -i "s/^INNER_MYSQL_USERNAME=.*/INNER_MYSQL_USERNAME=$MYSQL_USERNAME/" conf/configuration.properties
sed -i "s/^INNER_MYSQL_PASSWORD=.*/INNER_MYSQL_PASSWORD=$MYSQL_PASSWORD/" conf/configuration.properties
sed -i "s/^WEB_SERVICE\.IP=.*/WEB_SERVICE.IP=$WEB_SERVICE_IP/" conf/configuration.properties

sed -i "s/INNER_DataMining_MYSQL_DATABASENAME=.*/INNER_DataMining_MYSQL_DATABASENAME=datashire_DataMining/" conf/configuration.properties
sed -i "s/INNER_MYSQL_DATABASENAME=.*/INNER_MYSQL_DATABASENAME=datashire_database/" conf/configuration.properties

# 邮件
sed -i "s/^MAIL_PROTOCOL=.*/MAIL_PROTOCOL=$MAIL_PROTOCOL/" conf/configuration.properties
sed -i "s/^MAIL_HOST=.*/MAIL_HOST=$MAIL_HOST/" conf/configuration.properties
sed -i "s/^MAIL_PORT=.*/MAIL_PORT=$MAIL_PORT/" conf/configuration.properties
sed -i "s/^MAIL_USERNAME=.*/MAIL_USERNAME=$MAIL_USERNAME/" conf/configuration.properties
sed -i "s/^MAIL_PASSWORD=.*/MAIL_PASSWORD=$MAIL_PASSWORD/" conf/configuration.properties
sed -i "s/^MAIL_SMTP_AUTH=.*/MAIL_SMTP_AUTH=$MAIL_SMTP_AUTH/" conf/configuration.properties
sed -i "s/^MAIL_SMTP_STARTTLS_ENABLE=.*/MAIL_SMTP_STARTTLS_ENABLE=$MAIL_SMTP_STARTTLS_ENABLE/" conf/configuration.properties

# sed -i "s/^=.*/=$/" conf/configuration.properties