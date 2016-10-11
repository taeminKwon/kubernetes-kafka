# kubernetes-kafka

## forked from [CloudTrackInc/kubernetes-kafka](https://github.com/CloudTrackInc/kubernetes-kafka)

Apache Kafka to Kubernetes Replication Controller

Example of deploy cluster to kubernetes.

## Verification environment

* Ubuntu 16.04.1 LTS (Xenial Xerus)
* Docker version 1.12.1, build 23cf638
* VirtualBox 5.0.20 r106931
* Vagrant 1.8.4
* coreos-kubernetes vagrant info
  - https://github.com/coreos/coreos-kubernetes
  - https://github.com/coreos/coreos-kubernetes/tree/master/multi-node/vagrant

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
