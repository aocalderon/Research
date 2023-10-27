ThisBuild / version      := "0.1.0"
ThisBuild / organization := "edu.ucr.dblab"
ThisBuild / scalaVersion := "2.11.12"
crossScalaVersions := Seq("2.11.12", "2.12.17")

val SparkVersion = "2.4.0"

lazy val buildSettings = (project in file("."))
  .settings(
    name := "pflock",
    libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion,

    libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.19.0",

    libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0",

    libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1",
    libraryDependencies += "org.apache.commons" % "commons-geometry-enclosing" % "1.0-beta1",
    libraryDependencies += "org.apache.commons" % "commons-geometry-euclidean" % "1.0-beta1",
    libraryDependencies += "org.apache.commons" % "commons-numbers-parent" % "1.0-beta1",

    libraryDependencies += "org.rogach" %% "scallop" % "4.0.1",
    libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25",

    libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.32.0",
    
    libraryDependencies += "org.spire-math" %% "archery" % "0.6.0"

  )
