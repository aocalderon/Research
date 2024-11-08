ThisBuild / version      := "0.1.0"
ThisBuild / organization := "edu.ucr.dblab"
ThisBuild / scalaVersion := "2.11.12"

val SparkVersion = "2.4.0"

lazy val buildSettings = (project in file("."))
  .settings(
    name := "ICPE",
    libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion,
    libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % SparkVersion,
    libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25",
    libraryDependencies += "org.rogach" % "scallop_2.11" % "2.1.3",
    libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0",
    libraryDependencies += "org.datasyslab" % "JTSplus" % "0.1.4"

  )


