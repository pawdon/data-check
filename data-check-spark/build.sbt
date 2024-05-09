ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "data-check-spark"
  )

val sparkVersion = "3.5.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val hadoopVersion = "3.2.0"
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test
