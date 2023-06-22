name := "FF"
organization := "edu.ucr.dblab"
version := "0.1"

scalaVersion in ThisBuild := "2.11.12"
val SparkVersion = "2.4.0"
val SedonaVersion = "1.3.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion
libraryDependencies += "org.datasyslab" % "geospark" % SedonaVersion
//libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.18.0"

libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
libraryDependencies += "org.apache.commons" % "commons-geometry-enclosing" % "1.0-beta1"
libraryDependencies += "org.apache.commons" % "commons-geometry-euclidean" % "1.0-beta1"
libraryDependencies += "org.apache.commons" % "commons-numbers-parent" % "1.0-beta1"

libraryDependencies += "org.rogach" %% "scallop" % "4.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"
