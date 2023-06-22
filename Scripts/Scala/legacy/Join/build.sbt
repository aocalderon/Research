name := "DJoin"
organization := "edu.ucr.dblab"
version := "0.1"

scalaVersion in ThisBuild := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0"
libraryDependencies += "org.datasyslab" % "JTSplus" % "0.1.4"


mainClass in (Compile, run) := Some("edu.ucr.dblab.DJoin")
mainClass in (Compile, packageBin) := Some("edu.ucr.dblab.DJoin")
