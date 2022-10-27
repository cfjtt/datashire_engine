FROM daocloud.io/daocloud/centos-java:v0.4

MAINTAINER zhudebin "debin.zhu@eurlanda.com"

# 设置环境变量
# mysql 地址[ip,port],用户名,密码
# core-site.xml, yarn-site.xml, hdfs-site.xml
# hdfs ha services name
# SPARK_EXECUTOR_MEMORY
# SPARK_EXECUTOR_CORES
# ZOOKEEPER_ADDRESS
# WEB_SERVICE_IP

# 安装mysql 客户端
RUN yum install -y mysql  && mkdir /usr/local/datashire

# add server, engine, kafka
ADD server.tar.gz /usr/local/datashire/
ADD engine.tar.gz /usr/local/datashire/
ADD kafka.tar.gz /usr/local/datashire/
# expose port 9999 4040 10008
EXPOSE 9999 4040 10008

ADD startAll.sh /usr/local/datashire/
ADD init.tar.gz /usr/local/datashire/
RUN chmod a+x /usr/local/datashire/init/init.sh  && chmod a+x /usr/local/datashire/startAll.sh &&  chmod a+x /usr/local/datashire/engine/*.sh

# 直接加到开机启动项
RUN sed -i "$ a /bin/bash /usr/local/datashire/startAll.sh > /var/log/rc.log 2>&1 &" /etc/rc.d/rc.local

ENTRYPOINT ["/usr/local/datashire/startAll.sh"]