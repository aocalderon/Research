name := "pflogger"
organization := "org.andress"
version := "0.1"

scalaVersion in ThisBuild := "2.11.8"
val sparkVersion = "2.4.0" 

libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.rogach" %% "scallop" % "3.3.2"


mainClass in (Compile, run) := Some("org.andress.PFlogger")
mainClass in (Compile, packageBin) := Some("org.andress.PFLogger")
