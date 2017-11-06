FROM openjdk:jdk-alpine

ENV SCALA_VERSION 2.12.4
ENV Kafka_Broker localhost:9092
ENV Cassandra_Addresses localhost
ENV Cassandra_Port 9042
ENV Cassandra_Keyspace default
ENV Prefix default_prefix
ENV User default_user
ENV Password default_pass

ENV StatsD_Host none

WORKDIR /root
ADD target/scala-2.12/dumper.jar /root/dumper.jar
ADD etc/gelf.xml /root/gelf.xml
ADD etc/entrypoint.sh /root/entrypoint.sh
ENTRYPOINT ["/bin/sh","/root/entrypoint.sh"]