from eurlanda/centos-jdk8:1.0

LABEL maintainer="debin.zhu@eurlanda.com"
LABEL name="engine"
LABEL description="engine"
LABEL release-date="2018-09-03"
LABEL version="1.0"
WORKDIR /

RUN mkdir -p /opt/engine /opt/engine/hadoop_conf /opt/engine/engine-logs /opt/engine/conf

ADD target/engine.jar /opt/engine

ADD target/lib /opt/engine/lib

ADD doc/server-compose/initYarnJar.sh /opt/engine/

ADD doc/server-compose/start.sh /opt/engine/

ENV IP localhost

ENV HADOOP_USER_NAME hdfs

RUN echo "CONTAINER_IP=$IP"

RUN export SPARK_LOCAL_IP=$IP

RUN export SPARK_PUBLIC_DNS=$IP

RUN chmod +x /opt/engine/initYarnJar.sh

RUN chmod +x /opt/engine/start.sh

CMD cd /opt/engine && sh start.sh