name := "WeatherSimulator"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "1.6.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"    % sparkVersion,
  "org.apache.spark" %% "spark-graphx"  % sparkVersion,
  "joda-time" % "joda-time" % "2.9.4",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

mainClass in (Compile, packageBin) := Some("eu.fastdata.ws.WeatherSimulator")