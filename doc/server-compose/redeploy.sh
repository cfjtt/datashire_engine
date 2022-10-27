#! /bin/bash

docker-compose down
sh updateDockerImage.sh
docker-compose up -d
