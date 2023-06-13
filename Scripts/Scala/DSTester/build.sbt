name := "dstester"
organization := "edu.ucr.dblab"
version := "0.1"
scalaVersion in ThisBuild := "2.11.8"

libraryDependencies += "org.rogach" %% "scallop" % "4.0.1"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.6" % Test
libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.19.0"

// https://mvnrepository.com/artifact/io.github.elki-project/elki
libraryDependencies += "io.github.elki-project" % "elki" % "0.8.0"

// https://mvnrepository.com/artifact/org.apache.commons/commons-math3
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"

// https://mvnrepository.com/artifact/com.github.haifengl/smile-core
libraryDependencies += "com.github.haifengl" % "smile-core" % "3.0.1"
