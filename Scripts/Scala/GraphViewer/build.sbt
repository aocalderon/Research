name := "GraphViewer"
organization := "edu.ucr.dblab"
version := "0.1"

scalaVersion in ThisBuild := "2.11.8"

libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0"
libraryDependencies += "org.datasyslab" % "JTSplus" % "0.1.4"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"

mainClass in (Compile, run) := Some("edu.ucr.dblab.GraphViewer")
mainClass in (Compile, packageBin) := Some("edu.ucr.dblab.GraphViewer")
