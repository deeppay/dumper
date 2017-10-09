[![Build status](https://travis-ci.org/carldata/dumper.svg?branch=master)](https://travis-ci.org/carldata/dumper)

# Dumper


![icon](etc/dumper.jpg) 

Application for storing data from Kafka to Cassandra.

 
## Running test
 
 ```bash
sbt assembly
java -jar target/scala-2.12/dumper.jar --kafka=localhost:9092
 ```
 
# Redistributing

Dumper source code is distributed under the Apache-2.0 license.

**Contributions**

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
licensed as above, without any additional terms or conditions.
