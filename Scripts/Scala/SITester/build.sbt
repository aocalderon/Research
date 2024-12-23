ThisBuild / version      := "0.1.0"
ThisBuild / organization := "edu.ucr.dblab"
ThisBuild / scalaVersion := "2.11.12"

val SparkVersion = "2.4.0"

lazy val buildSettings = (project in file("."))
  .settings(
    name := "sitester",

    libraryDependencies += "org.rogach" %% "scallop" % "4.0.1",
    libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25",
    libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.19.0"
  )
