ThisBuild / version      := "0.1.0"
ThisBuild / organization := "edu.ucr.dblab"
ThisBuild / scalaVersion := "2.11.12"

lazy val buildSettings = (project in file("."))
  .settings(
    name := "parrouter",
    resolvers += "osgeo" at "https://repo.osgeo.org/repository/release/",
    libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.19.0",
    libraryDependencies += "com.graphhopper" % "graphhopper-core" % "7.0",
    libraryDependencies += "org.geotools" % "gt-main" % "29.1",
    libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.32.0",

    libraryDependencies += "org.rogach" %% "scallop" % "4.0.1",
    libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"
  )
