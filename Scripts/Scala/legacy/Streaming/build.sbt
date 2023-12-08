ThisBuild / version      := "0.1.0"
ThisBuild / organization := "edu.ucr.dblab"
ThisBuild / scalaVersion := "2.11.12"

lazy val buildSettings = (project in file("."))
  .settings(
    name := "streaming",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0",
    libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.0",
    libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25",
    libraryDependencies += "org.rogach" % "scallop_2.11" % "2.1.3"
  )
