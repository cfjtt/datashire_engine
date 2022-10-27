#! /bin/bash
cd $ds_path/server

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

if [ -d server_jar ]; then
  rm -Rf server_jar
fi

mkdir server_jar
cp datashire_server.jar server_jar/
cd server_jar

jar xf datashire_server.jar

#修改配置文件
source $ds_path/init/initServer_conf.sh

# 将配置文件夹整体更新到jar
jar uf datashire_server.jar config

# 覆盖server.jar

cd ..
echo $(pwd)
rm -f server.jar
cp server_jar/datashire_server.jar ./
#rm -Rf server_jar