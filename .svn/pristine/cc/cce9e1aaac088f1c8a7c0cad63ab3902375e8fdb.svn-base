#! /bin/bash

# 初始化环境变量,没有设置,采用默认值
hostname=$(hostname)

function exitWithError() {
  echo "异常退出:$1" >> $ds_path/log/ds_install.log
  exit 1
}

function init_env() {

    if [ ! -n "$ZOOKEEPER_ADDRESS" ]; then
      exitWithError "请设置ZOOKEEPER_ADDRESS"
    fi

    if [ ! -n "$MYSQL_HOST" ]; then
      exitWithError "请设置 MYSQL_HOST"
    fi

    if [ ! -n "$MYSQL_PORT" ]; then
      exitWithError "请设置 MYSQL_PORT"
    fi
    if [ ! -n "$MYSQL_USERNAME" ]; then
      exitWithError "请设置 MYSQL_USERNAME"
    fi

    if [ ! -n "$MYSQL_PASSWORD" ]; then
      exitWithError "请设置 MYSQL_PASSWORD"
    fi

}

init_env;

sed -i "s#^jdbc.url=.*#jdbc.url=jdbc:mysql://$MYSQL_HOST:$MYSQL_PORT/datashire_database#" config/config.properties
sed -i "s#^jdbc.username=.*#jdbc.username=$MYSQL_USERNAME#" config/config.properties
sed -i "s#^jdbc.password=.*#jdbc.password=$MYSQL_PASSWORD#" config/config.properties

sed -i "s/KAFKA_BROKER_LIST=.*/KAFKA_BROKER_LIST=$hostname:9092/" config/config.properties
sed -i "s/^ZOOKEEPER_ADDRESS=.*/ZOOKEEPER_ADDRESS=$ZOOKEEPER_ADDRESS/" config/config.properties

sed -i "s#^Hsql_Host_Address=.*#Hsql_Host_Address=jdbc:mysql://$MYSQL_HOST:$MYSQL_PORT/#" config/config.properties
sed -i "s#^HSQL_USER_NAME=.*#HSQL_USER_NAME=$MYSQL_USERNAME#" config/config.properties
sed -i "s#^HSQL_PASSWORD=.*#HSQL_PASSWORD=$MYSQL_PASSWORD#" config/config.properties
sed -i "s#^HsqlDB_SysName=.*#HsqlDB_SysName=datashire_database#" config/config.properties
sed -i "s#^hbase.dbname=.*#hbase.dbname=datashire_DataMining#" config/config.properties


sed -i "s#^rpc.engine.addr=.*#rpc.engine.addr=127.0.0.1#" config/config.properties

sed -i "s#^hbase.port=.*#hbase.port=$MYSQL_PORT#" config/config.properties
sed -i "s#^hbase.host=.*#hbase.host=$MYSQL_HOST#" config/config.properties
sed -i "s#^hbase.password=.*#hbase.password=$MYSQL_PASSWORD#" config/config.properties
sed -i "s#^hbase.username=.*#hbase.username=$MYSQL_USERNAME#" config/config.properties

# sed -i "s/^=.*/=$/" config/config.properties