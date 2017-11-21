FROM openjdk:jdk-alpine

ENV SCALA_VERSION 2.12.4
ENV KAFKA localhost:9092
ENV DB localhost
ENV DB_PORT 9042
ENV KEYSPACE default
ENV PREFIX default_prefix
ENV USER default_user
ENV PASS default_pass
ENV STATSD_HOST none

WORKDIR /root
ADD target/scala-2.12/dumper.jar /root/dumper.jar
ADD etc/gelf.xml /root/gelf.xml
ADD etc/entrypoint.sh /root/entrypoint.sh
ENTRYPOINT ["/bin/sh","/root/entrypoint.sh"]