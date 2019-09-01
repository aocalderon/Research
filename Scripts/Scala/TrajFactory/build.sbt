name := "TrajFactory"
organization := "UCR-DBLab"
version := "0.1"

scalaVersion in ThisBuild := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25"
libraryDependencies += "org.rogach" % "scallop_2.11" % "2.1.3"
libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0"
libraryDependencies += "org.datasyslab" % "JTSplus" % "0.1.4"
libraryDependencies += "com.graphhopper" % "graphhopper-reader-osm" % "0.12.0"
libraryDependencies += "com.graphhopper" % "graphhopper-core" % "0.12.0"

mainClass in (Compile, run) := Some("TrajFactory")
mainClass in (Compile, packageBin) := Some("TrajFactory")
