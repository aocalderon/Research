name := "utils"
organization := "org.andress"
version := "0.1"
scalaVersion in ThisBuild := "2.11.12"

libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25"

mainClass in (Compile, run) := Some("org.andress.Utils")
mainClass in (Compile, packageBin) := Some("org.andress.Utils")
