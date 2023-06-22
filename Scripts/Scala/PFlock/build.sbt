ThisBuild / version      := "0.1.0"
ThisBuild / organization := "edu.ucr.dblab"
ThisBuild / scalaVersion := "2.11.12"

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

lazy val cpCP = taskKey[Unit]("Copy classpath to lib folder...")
cpCP := {
  import sys.process._
  val cp: Seq[File] = (fullClasspathAsJars in Runtime).value.files
  val base = baseDirectory.value
  cp.foreach{ c =>
    println(s"Copy $c ...")
    s"cp $c ${base}/lib" !
  }
}
 
lazy val scalaBash = taskKey[Unit]("Create a scala bash script...")
scalaBash := {
  import sys.process._
  val cp: Seq[File] = (fullClasspathAsJars in Runtime).value.files
  val base = baseDirectory.value
  val strCP = cp.mkString(":")
  val finder: PathFinder = (base / "target") ** "*.jar" 
  val jar = finder.get.mkString(":")

  println(s"scala -cp ${strCP}:${jar} edu.ucr.dblab.pflock.BFE")
}

lazy val sparkBash = taskKey[Unit]("Create a spark bash script...")
sparkBash := {
  import sys.process._
  val cp: Seq[File] = (fullClasspathAsJars in Runtime).value.files
  val base = baseDirectory.value
  val finder: PathFinder = (base / "target") ** "*.jar" 
  val jar = finder.get.mkString(" ")
  val log_file = s"${System.getProperty("user.home")}/Spark/2.4/conf/log4j.properties"

  val modules = libraryDependencies.value.map(_.toString).filterNot(_.contains("spark")).filterNot(_.contains("scala-library"))
  println(modules.mkString("\n"))
  val strCP = for{
    module <- modules.map(_.split(":")(1))
    path <- cp.map(_.toString)
    if { path.contains(module) }
  } yield { path }

  val bash = List(
    s"#!/bin/bash \n\n",
    s" spark-submit",
    s" --files $log_file",
    s" --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$log_file",
    s" --jars ${strCP.mkString(",")}",
    s" --master local[*]",
    s" --class edu.ucr.dblab.pflock.MF ${jar} --input ~/Research/Datasets/dense.tsv --density 0"  
  ).mkString("")

  val f = new java.io.PrintWriter("lib/mf.sh")
  f.write(bash)
  f.close
}
