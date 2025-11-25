ThisBuild / organization := "puj"
ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.12.20"

val SparkVersion = "3.5.7"

lazy val buildSettings = (project in file("."))
  .settings(
    name := "pflock",
    libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion,
    libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.20.0",
    libraryDependencies += "org.rogach" %% "scallop" % "5.3.0",
    libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0"
  )