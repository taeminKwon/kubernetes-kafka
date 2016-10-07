# kubernetes-kafka
Apache Kafka to Kubernetes Replication Controller

Example of deploy cluster to kubernetes.

## Features
* Support scaling
* Auto rebalancing on expanding cluster

## Example

* Replication Controller - kafka-rc.yaml
* Service - kafka-service.yaml

## Run on kubernetes:

Create kubernetes kafka replication controller.

```
# kubectl create -f kafka-rc.yaml
```

Create kubernetes kafka service.

```
# kubectl create -f kafka-service.yaml
```

Don't forget set ENABLE_AUTO_EXTEND environment variable to true.
