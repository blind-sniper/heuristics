#!/bin/bash
CONTAINERS=$(docker ps -a | grep -v "kafka|mongo|aerospike" | awk '{print $NF}')
TIME=$(date +%Y%m%d%H%M%S)
LOGS_DIR=logs/$TIME
mkdir -p $LOGS_DIR
for container in ${CONTAINERS[@]}; do
  echo $container
  cat <<< "$(docker logs $container 2>&1)" > $LOGS_DIR/$container.log
done
tar cvzf logs-$TIME.tar.gz $LOGS_DIR
