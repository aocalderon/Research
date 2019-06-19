name := "dyn4j-Tester"
organization := "UCR-DBLab"
version := "0.1"

scalaVersion in ThisBuild := "2.11.8"

libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25"
libraryDependencies += "org.rogach" % "scallop_2.11" % "2.1.3"
libraryDependencies += "org.dyn4j" % "dyn4j" % "3.3.0"

mainClass in (Compile, run) := Some("Tester")
mainClass in (Compile, packageBin) := Some("Tester")
