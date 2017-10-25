#!/bin/sh
java -Dlogback.configurationFile=/root/logback_deployment.xml -jar /root/dumper.jar --db=$Cassandra_Addresses --dbPort=$Cassandra_Port --keyspace=$Cassandra_Keyspace --kafka=$Kafka_Broker --prefix=$Prefix --user=$User --pass=$Password  --statSDHost=$StatSD_Host