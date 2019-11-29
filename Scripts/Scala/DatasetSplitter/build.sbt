name := "DatasetSplitter"
organization := "UCR-DBLab"
version := "0.1"

scalaVersion in ThisBuild := "2.11.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25"
libraryDependencies += "org.rogach" % "scallop_2.11" % "2.1.3"

mainClass in (Compile, run) := Some("DatasetSplitter")
mainClass in (Compile, packageBin) := Some("DatasetSplitter")
