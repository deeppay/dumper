name := "dumper"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.11.0.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "de.siegmar" % "logback-gelf" % "1.0.4",
  "io.github.carldata" %% "hydra-streams" % "0.5.0",
  "com.datadoghq" % "java-dogstatsd-client" % "2.3",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Runtime,

  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "dumper.jar"
