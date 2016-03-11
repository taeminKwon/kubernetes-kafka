#!/usr/bin/env bash


BROKER="$KAFKA_ADVERTISED_HOST_NAME:$KAFKA_ADVERTISED_PORT"
exec $KAFKA_HOME/bin/kafka-run-class.sh kafka.admin.AutoExpandCommand --zookeeper=$KAFKA_ZOOKEEPER_CONNECT --broker=$BROKER --updown=$1