#!/bin/sh
java -jar /root/dumper.jar --db=$Cassandra_Address --keyspace=$Cassandra_Keyspace --kafka=$Kafka_Broker --prefix=$Prefix --user=$User --pass=$Password