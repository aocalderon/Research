name := "PLCM"
organization := "UCR-DBLab"
version := "0.1"

scalaVersion in ThisBuild := "2.11.8"

val SparkVersion = "2.3.0"
val GeoSparkVersion = "1.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion
libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25"
libraryDependencies += "org.rogach" % "scallop_2.11" % "2.1.3"
libraryDependencies += "org.datasyslab" % "geospark" % GeoSparkVersion
libraryDependencies += "org.datasyslab" % "JTSplus" % "0.1.4"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.1.7"

mainClass in (Compile, run) := Some("PLCM")
mainClass in (Compile, packageBin) := Some("PLCM")
