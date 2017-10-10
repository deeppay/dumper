FROM openjdk:jdk-alpine

ENV SCALA_VERSION 2.12.3
ENV Kafka_Broker localhost:9092
ENV Cassandra_Address localhost:9042
ENV Cassandra_Keyspace default
ENV Prefix default_prefix
ENV User default_user
ENV Password default_pass

WORKDIR /root
ADD target/scala-2.12/dumper.jar /root/dumper.jar
ADD entrypoint.sh /root/entrypoint.sh
ENTRYPOINT ["/bin/sh","/root/entrypoint.sh"]