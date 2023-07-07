ThisBuild / version      := "0.1.0"
ThisBuild / organization := "edu.ucr.dblab"
ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

lazy val buildSettings = (project in file("."))
  .settings(
    name := "tester3d",
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion,

    libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.19.0",

    libraryDependencies += "org.rogach" %% "scallop" % "4.0.1",

    libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25",

    libraryDependencies += "tech.tablesaw" % "tablesaw-core" % "0.43.1",
    libraryDependencies += "tech.tablesaw" % "tablesaw-jsplot" % "0.43.1",

    libraryDependencies += "net.openhft" % "zero-allocation-hashing" % "0.16"
  )
