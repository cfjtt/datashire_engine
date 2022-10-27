#! /bin/bash
hadoop fs -chmod -R 777 /user
docker pull 192.168.137.215:5000/eurlanda/server:2.2.4.0-SNAPSHOT

docker tag 192.168.137.215:5000/eurlanda/server:2.2.4.0-SNAPSHOT  eurlanda/server:2.2.4.0

docker pull 192.168.137.215:5000/eurlanda/engine:2.2.4.0-SNAPSHOT

docker tag 192.168.137.215:5000/eurlanda/engine:2.2.4.0-SNAPSHOT eurlanda/engine:2.2.4.0
