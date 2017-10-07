name := "dumper"

version := "0.1"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.11.0.1",
  "org.slf4j" % "slf4j-log4j12" % "1.8.0-alpha2",
  "io.github.carldata" %% "hydra-streams" % "0.4.3",

  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "net.manub" %% "scalatest-embedded-kafka" % "0.15.1" % "test"
)

assemblyJarName in assembly := "dumper.jar"