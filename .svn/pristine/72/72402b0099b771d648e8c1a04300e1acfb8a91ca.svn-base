#! /bin/bash
cd $ds_path/kafka

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

hostname=$(hostname)
# sed -i "s#^=.*#=$#" config/server.properties

mkdir -p $ds_path/log/kafka-logs

# 判断 ZOOKEEPER_ADDRESS 是否存在
if [ ! -n "$ZOOKEEPER_ADDRESS" ]; then
  exitWithError "异常退出:请设置 ZOOKEEPER_ADDRESS"
fi

sed -i "s#^broker.id=.*#broker.id=1#" config/server.properties
sed -i "s#^port=.*#port=9092#" config/server.properties
sed -i "s#^host.name=.*#host.name=$hostname#" config/server.properties
sed -i "s#^zookeeper.connect=.*#zookeeper.connect=$ZOOKEEPER_ADDRESS#" config/server.properties
