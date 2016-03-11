FROM wurstmeister/kafka

MAINTAINER CloudTrackInc


RUN ./gradlew build

RUN cp build/libs/kubernetes-expander-1.0-SNAPSHOT $KAFKA_HOME/libs

ADD kafka-autoextend-partitions.sh /usr/bin/kafka-autoextend-partitions.sh
