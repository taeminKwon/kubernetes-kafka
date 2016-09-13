#!/usr/bin/env bash


if [[ -z "${KAFKA_ADVERTISED_HOST_NAME// }" ]] ; then
	echo "KAFKA_ADVERTISED_HOST_NAME NOT DEFINED OR INVALID '$KAFKA_ADVERTISED_HOST_NAME' !!!"
	exit 1
fi


if [[ -z "$KAFKA_BROKER_ID" ]]; then
	if [[ -z "$KUBERNETS_UID" ]]; then
		export KUBERNETS_UID=$HOSTNAME
	fi
    echo "Generate Kafka Broker ID: for $KUBERNETS_UID"
	ID=`$KAFKA_HOME/bin/kafka-run-class.sh kafka.admin.AutoExpandCommand --zookeeper=$KAFKA_ZOOKEEPER_CONNECT -broker=$KUBERNETS_UID -mode=generate`
	if [[ -z "$ID" ]]; then
		echo "Got empty broker ID from kafka.admin.AutoExpandCommand; not starting!"
		exit 1
	fi
	echo "Use broker ID: $ID"
	export KAFKA_BROKER_ID=$ID
fi

if [[ -n "$ENABLE_AUTO_EXTEND" ]]; then
	echo "Enable auto exand"
	/usr/bin/kafka-autoextend-partitions.sh &
	/usr/bin/start-kafka.sh
else
	/usr/bin/start-kafka.sh
fi