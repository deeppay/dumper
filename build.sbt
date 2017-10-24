name := "dumper"

version := "0.1"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.11.0.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0",
  "org.slf4j" % "slf4j-log4j12" % "1.8.0-alpha2",
  "de.siegmar" % "logback-gelf" % "1.0.4",
  "io.github.carldata" %% "hydra-streams" % "0.4.4",
  "com.datadoghq" % "java-dogstatsd-client" % "2.3",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Runtime,

  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "dumper.jar"
