ThisBuild / version      := "0.1.0"
ThisBuild / organization := "edu.ucr.dblab"
ThisBuild / scalaVersion := "2.11.12"
crossScalaVersions := Seq("2.11.12", "2.12.17")

val SparkVersion = "2.4.0"

lazy val hello = (project in file("."))
  .settings(
    name := "pflock",
    libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion,

    libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.19.0",

    libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0",

    libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1",
    libraryDependencies += "org.apache.commons" % "commons-geometry-enclosing" % "1.0-beta1",
    libraryDependencies += "org.apache.commons" % "commons-geometry-euclidean" % "1.0-beta1",
    libraryDependencies += "org.apache.commons" % "commons-numbers-parent" % "1.0-beta1",

    libraryDependencies += "org.rogach" %% "scallop" % "4.0.1",
    libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25",

    libraryDependencies += "org.spire-math" %% "archery" % "0.6.0"
  )

lazy val sparkBash = taskKey[Unit]("Create a spark bash script...")
sparkBash := {
  import sys.process._
  val classpath: Seq[File] = (Runtime / fullClasspathAsJars).value.files

  val modules = libraryDependencies.value.map(_.toString).filterNot(_.contains("spark")).filterNot(_.contains("scala-library"))
  println(modules.mkString("\n"))
  val jar_paths = for{
    module <- modules.map(_.split(":")(1))
    path   <- classpath.map(_.toString)
    if { path.contains(module) }
  } yield { path }

  val finder: PathFinder = (baseDirectory.value / "target") ** "*.jar"
  val jar = finder.get.mkString(" ")

  val log_file = s"${System.getProperty("user.home")}/Spark/2.4/conf/log4j.properties "
  val files    = s"$log_file "
  val conf     = s"spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$log_file "
  val jars     = s"${jar_paths.mkString(",")} "
  val master   = s"local[*] "
  val aclass   = s"edu.ucr.dblab.pflock.MF "

  val bash = List(
    s"#!/bin/bash \n",
    s"PARAMS=(",
    s" --files  $files \\",
    s" --conf   $conf \\",
    s" --jars   $jars \\",
    s" --master $master \\",
    s" --class  $aclass ",
    s")\n",
    s"spark-submit $${PARAMS[@]} $jar $$* \n"
  ).mkString("\n")

  val f = new java.io.PrintWriter("bash/mf")
  f.write(bash)
  f.close
}

lazy val cpCP = taskKey[Unit]("Copy classpath to lib folder...")
cpCP := {
  import sys.process._
  val cp: Seq[File] = (Runtime / fullClasspathAsJars).value.files
  val base = baseDirectory.value
  cp.foreach{ c =>
    println(s"Copy $c ...")
    s"cp $c /home/and/Spark/2.4/jars/" !
  }
}
 
lazy val scalaBash = taskKey[Unit]("Create a scala bash script...")
scalaBash := {
  import sys.process._
  val cp: Seq[File] = (Runtime / fullClasspathAsJars).value.files
  val base = baseDirectory.value
  val strCP = cp.mkString(":")

  val bash = s"scala -cp ${strCP} edu.ucr.dblab.pflock.MF $$*"
  val f = new java.io.PrintWriter("bash/mf_scala")
  f.write(bash)
  f.close
}

lazy val javaBash = taskKey[Unit]("Create a java bash script...")
javaBash := {
  import sys.process._
  val cp: Seq[File] = (Runtime / fullClasspathAsJars).value.files
  val base = baseDirectory.value
  val strCP = cp.mkString(":")

  val bash = s"java -cp ${strCP} edu.ucr.dblab.pflock.MF $$*"
  val f = new java.io.PrintWriter("bash/mf_java")
  f.write(bash)
  f.close
}
