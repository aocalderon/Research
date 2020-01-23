name := "utils"
organization := "org.andress"
version := "0.1"
scalaVersion in ThisBuild := "2.12.10"

libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25"
libraryDependencies += "co.theasi" %% "plotly" % "0.2.0"

mainClass in (Compile, run) := Some("org.andress.Utils")
mainClass in (Compile, packageBin) := Some("org.andress.Utils")
