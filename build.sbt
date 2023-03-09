name := "scala-engineer-test"

version := "0.1"

scalaVersion := "2.13.10"

libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
  "org.scalatest" %% "scalatest" % "3.2.11" % Test,
  "org.scalatest" %% "scalatest-funsuite" % "3.3.0-SNAP2" % Test
)