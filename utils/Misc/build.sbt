name := "utils"
organization := "edu.ucr.dblab"
version := "0.1"
scalaVersion in ThisBuild := "2.11.8"

libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25"

mainClass in (Compile, run) := Some("edu.ucr.dblab.Utils")
mainClass in (Compile, packageBin) := Some("edu.ucr.dblab.Utils")
