name := "sparkEventsAnalytics"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "org.scalatest" %% "scalatest" % "3.0.5",
  "mysql" % "mysql-connector-java" % "8.0.12"
)