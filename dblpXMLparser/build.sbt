name := "dblpXMLparser"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "1.3.0",
  "com.typesafe" % "config" % "1.4.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)

fork := true