#!/usr/bin/env bash

echo "Generate Kafka Broker ID"
function join { local IFS="$1"; shift; echo "$*"; }
A=(${KAFKA_ADVERTISED_HOST_NAME//\./ })
ID=`join '' ${A[@]:1}`
ID=${ID##*0}
echo "Use broker ID: $ID"

export KAFKA_BROKER_ID=$ID

/usr/bin/start-kafka.sh

