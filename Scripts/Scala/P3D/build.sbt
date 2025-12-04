ThisBuild / organization := "puj"
ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.12.20"

val SparkVersion = "3.5.7"

lazy val buildSettings = (project in file("."))
  .settings(
    name := "pflock",
    libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion,
    libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.20.0",
    libraryDependencies += "org.rogach" %% "scallop" % "5.3.0",
    libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0",
    libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "13.1.0"
  )

fork := true   // enabling forking to apply JVM options to the app, not just the sbt shell...

// Adding the necessary JVM options
javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)
