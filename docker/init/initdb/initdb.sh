#! /bin/bash
BASE_DIR=$(cd `dirname $0`; pwd)
echo $BASE_DIR
cd $BASE_DIR

# 判断是否存在该数据库
mysql -h$MYSQL_HOST -u$MYSQL_USERNAME -p$MYSQL_PASSWORD -e "show databases;" |grep -q datashire_database
if [ $? -ne 0 ]; then
  mysql -h$MYSQL_HOST -u$MYSQL_USERNAME -p$MYSQL_PASSWORD <<EOF >/dev/null
    CREATE DATABASE datashire_database DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
EOF
  mysql -h$MYSQL_HOST -u$MYSQL_USERNAME -p$MYSQL_PASSWORD datashire_database < init.sql
else
  echo "存在数据库 datashire_database, 不执行创建和初始化动作"  >> /var/log/ds_install.log
fi

mysql -h$MYSQL_HOST -u$MYSQL_USERNAME -p$MYSQL_PASSWORD -e "show databases;" |grep -q datashire_datamining
if [ $? -ne 0 ]; then
  mysql -h$MYSQL_HOST -u$MYSQL_USERNAME -p$MYSQL_PASSWORD <<EOF >/dev/null
    CREATE DATABASE datashire_datamining DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
EOF
else
  echo "存在数据库 datashire_datamining, 不执行创建和初始化动作"  >> /var/log/ds_install.log
fi
