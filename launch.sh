#!/bin/bash
set -e

export COMPOSE_HTTP_TIMEOUT=900

./build.sh

RANKING_MODE=${1:-random}
RANKING_OPTS=${2:-}
PSEUDOFEEDBACK_OPT="${3:-0} ${4:-}"

echo [INFO] Ranking mode: $RANKING_MODE
echo [INFO] Ranking opts: $RANKING_OPTS
echo [INFO] Pseudofeedback opt: $PSEUDOFEEDBACK_OPT

SCENARIO_DIR=${RANKING_MODE}${RANKING_OPTS}${3:-}${4:-}
sudo rm -rf $SCENARIO_DIR
mkdir $SCENARIO_DIR
sed "s/command: user_chooser.py -k kafka:9092 -m mongodb:27017/command: user_chooser.py -k kafka:9092 -m mongodb:27017 ${RANKING_MODE} ${RANKING_OPTS}/g" docker-compose.yaml.template > $SCENARIO_DIR/docker-compose.yaml
if [ ! -z "$PSEUDOFEEDBACK_OPT" ]; then
  sed -i "s/command: model_trainer.py -k kafka:9092 -i users_texts/command: model_trainer.py -k kafka:9092 -i users_texts ${PSEUDOFEEDBACK_OPT}/g" $SCENARIO_DIR/docker-compose.yaml
  sed -i "s/command: batch_proba_updater.py -k kafka:9092 -m mongodb:27017/command: batch_proba_updater.py -k kafka:9092 -m mongodb:27017 ${PSEUDOFEEDBACK_OPT}/g" $SCENARIO_DIR/docker-compose.yaml
fi

cd $SCENARIO_DIR
docker-compose --compatibility down
docker-compose --compatibility up -d \
--scale user_extractor=4 \
--scale subreddit_explorer=1 \
--scale user_classifier=1 &
sleep 5
