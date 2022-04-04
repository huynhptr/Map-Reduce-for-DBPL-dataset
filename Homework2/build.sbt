name := "Homework2"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "3.3.0",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "org.scala-lang.modules" %% "scala-xml" % "1.3.0",
  "com.typesafe" % "config" % "1.4.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test"
)

fork := true

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}