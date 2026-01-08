val sparkVersion = "3.5.7"

lazy val root = project
  .in(file("."))
  .settings(
    name         := "MoSTTrajManager",
    version      := "0.1",
    scalaVersion := "2.12.20",
    libraryDependencies ++= Seq(
      "org.apache.spark"         %% "spark-core"      % sparkVersion,
      "org.apache.spark"         %% "spark-sql"       % sparkVersion,
      "org.locationtech.jts"      % "jts-core"        % "1.20.0",
      "org.locationtech.proj4j"   % "proj4j"          % "1.1.0",
      "com.github.scopt"         %% "scopt"           % "4.1.0",
      "org.apache.logging.log4j"  % "log4j-core"      % "2.24.1",
      "org.apache.logging.log4j" %% "log4j-api-scala" % "13.1.0",
      "org.scalatest"            %% "scalatest"       % "3.2.17" % Test,
      "org.scalameta"            %% "munit"           % "1.0.0"  % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
    fork := true, // enabling forking to apply JVM options to the app, not just the sbt shell...
    javaOptions ++= Seq(
      "-Xmx128G",
      "-Xms128G",
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
  )
