# kubernetes-kafka

## forked from [CloudTrackInc/kubernetes-kafka](https://github.com/CloudTrackInc/kubernetes-kafka)

Apache Kafka to Kubernetes Replication Controller

Example of deploy cluster to kubernetes.

## Features
* Support scaling
* Auto rebalancing on expanding cluster

## Example

* Replication Controller - kafka-rc.yaml
* Service - kafka-service.yaml

Don't forget set ENABLE_AUTO_EXTEND environment variable to true.

## Run on kubernetes:

Create kubernetes kafka replication controller.

```
# kubectl create -f kafka-rc.yaml
```

Create kubernetes kafka service.

```
# kubectl create -f kafka-service.yaml
```

## Configure edit kafka-service.yaml

If you need to set up kafka-zoo-svc on kubernetes, you will setup this point.

```
- name: KAFKA_ZOOKEEPER_CONNECT
value: kafka-zoo-svc:2181
```

If you want to create topic, you will change this point.

```
- name: KAFKA_CREATE_TOPICS
value: metrics:16:1
```
