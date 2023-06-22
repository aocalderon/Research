name := "sitester"
organization := "edu.ucr.dblab"
version := "0.1"
scalaVersion in ThisBuild := "2.11.8"

libraryDependencies += "org.rogach" %% "scallop" % "4.0.1"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"
libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.19.0"

val mySourceGenerator = taskKey[Seq[File]](...)

Compile / mySourceGenerator :=
  generate( (Compile / sourceManaged).value / "some_directory")

Compile / sourceGenerators += (Compile / mySourceGenerator)
