import complete.DefaultParsers._
import sys.process._
  
lazy val sparkBash = inputKey[Unit]("Create a spark bash script...")
sparkBash := {

  val args: Seq[String] = spaceDelimited("<arg>").parsed

  val classpathJars: Seq[File] = (Runtime / fullClasspathAsJars).value.files
  val modules = libraryDependencies.value.map(_.toString).filterNot(_.contains("spark")).filterNot(_.contains("scala-library"))
  val cpJars_paths = for{
    module <- modules.map(_.split(":")(1))
    path   <- classpathJars.map(_.toString)
    if { path.contains(module) }
  } yield { path }
  cpJars_paths.map{_.toString.split("/").last}.foreach{println}

  val libJars_paths: Seq[File] = (Runtime / unmanagedJars).value.files
  libJars_paths.map{_.getName}.foreach{println}

  val finder: PathFinder = (baseDirectory.value / "target") ** "*.jar"
  val jar = finder.get.mkString(" ")

  val log_file  = s"${System.getProperty("user.home")}/Spark/2.4/conf/log4j.properties "
  val files     = s"$log_file "
  val conf      = s"spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$log_file "
  val jars      = s"${cpJars_paths.mkString(",")},${libJars_paths.mkString(",")} "
  val master    = s"local[*] "
  val classname = s"${args(0)} "

  val bash = List(
    s"#!/bin/bash \n",
    s"PARAMS=(",
    s" --files  $files \\",
    s" --conf   $conf \\",
    s" --jars   $jars \\",
    s" --master $master \\",
    s" --class  $classname ",
    s")\n",
    s"spark-submit $${PARAMS[@]} $jar $$* \n"
  ).mkString("\n")

  val f = new java.io.PrintWriter(s"bash/${classname.split("\\.").last.toLowerCase.trim}")
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
    s"cp $c ${base}/lib" !
  }
}

lazy val scalaBash = taskKey[Unit]("Create a scala bash script...")
scalaBash := {
  import sys.process._
  val cp: Seq[File] = (Runtime / fullClasspathAsJars).value.files
  val base = baseDirectory.value
  val strCP = cp.mkString(":")
  val finder: PathFinder = (base / "target") ** "*.jar"
  val jar = finder.get.mkString(":")

  println(s"scala -cp ${strCP}:${jar} edu.ucr.dblab.pflock.BFE")
}
