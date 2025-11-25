import complete.DefaultParsers._
import sys.process._
import java.io.FileWriter
import java.nio.file.{Files, Paths}
  
lazy val sparkBash = inputKey[Unit]("Create a spark bash script...")
sparkBash := {

  val args: Seq[String] = spaceDelimited("<arg>").parsed

  val classpathJars: Seq[File] = (Runtime / fullClasspathAsJars).value.files
  val modules = libraryDependencies.value.map(_.toString)
    .filterNot(_.contains("spark")).filterNot(_.contains("scala-library"))
  val cpJars_paths = for{
    module <- modules.map(_.split(":")(1))
    path   <- classpathJars.map(_.toString)
    if { path.contains(module) }
  } yield { path }
  cpJars_paths.map{_.toString.split("/").last}.foreach{println}

  val libJars_paths: Seq[File] = (Runtime / unmanagedJars).value.files
  libJars_paths.map{_.getName}.foreach{println}

  val finder: PathFinder = (baseDirectory.value / "target") ** "*.jar"
  val jar = finder.get.last

  val log_file  = s"${System.getProperty("user.home")}/Spark/2.4/conf/log4j.properties "
  val files     = s"$log_file "
  val conf      = s"spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$log_file "
  val jars      = s"${cpJars_paths.mkString(",")},${libJars_paths.mkString(",")} "
  val master    = s"local[*] "
  val classname = s"${args(0)} "

  val bash = List(
    s"PARAMS=(",
    s" --files  $files \\",
    s" --conf   $conf \\",
    s" --jars   $jars \\",
    s" --master $master \\",
    s" --class  $classname ",
    s")\n",
    s"spark-submit $${PARAMS[@]} $jar $$* \n"
  ).mkString("\n")

  val script_name = classname.split("\\.").last.toLowerCase.trim
  val script = new File(s"bash/${script_name}_spark")
  script.setExecutable(true, true)
  val f = new FileWriter(script)
  f.write("#/usr/bin/bash \n\n")
  f.write(bash)
  f.close
}

lazy val copyClasspath = inputKey[Unit]("Copy classpath to lib folder...")
copyClasspath := {

  val args: Seq[String] = spaceDelimited("<arg>").parsed

  val dir_output = Files.createDirectories(Paths.get(args(0)))
  val cp: Seq[File] = (Runtime / fullClasspathAsJars).value.files
  val base = baseDirectory.value
  cp.foreach{ c =>
    println(s"Copy $c ...")
    s"cp $c ${dir_output}" !
  }
}

lazy val scalaBash = inputKey[Unit]("Create a scala bash script...")
scalaBash := {

  val args: Seq[String] = spaceDelimited("<arg>").parsed

  val cp: Seq[File] = (Runtime / fullClasspathAsJars).value.files
  val base = baseDirectory.value
  val strCP = cp.map{ j => s"""\t"${j.toString}"""" }.mkString("\n")
  val finder: PathFinder = (base / "target") ** "*.jar"
  val jar = finder.get.mkString(":")
  val classname = args(0)

  val script_name = classname.split("\\.").last.toLowerCase.trim
  val script = new File(s"bash/${script_name}_scala")
  script.setExecutable(true, true)
  val f = new FileWriter(script)
  f.write(s"""#/usr/bin/bash \n\n""")
  f.write(s"""JARS=(\n${strCP}\n)\n""")
  f.write(s"""function join_by { local IFS="$$1"; shift; echo "$$*"; }\n""")
  f.write(s"""THE_CLASSPATH=$$(join_by : "$${JARS[@]}")\n""")
  f.write(s"""java -cp $$THE_CLASSPATH $classname $$*\n""")
  f.close
}

lazy val javaBash = inputKey[Unit]("Create a java bash script...")
javaBash := {

  val args: Seq[String] = spaceDelimited("<arg>").parsed

  val cp: Seq[File] = (Runtime / fullClasspathAsJars).value.files
  val base = baseDirectory.value
  val strCP = cp.map{ j => s"""\t"${j.toString}"""" }.mkString("\n")
  val finder: PathFinder = (base / "target") ** "*.jar"
  val jar = finder.get.mkString(":")
  val classname = args(0)

  val bash = List(
    s"JARS=(",
    s"${strCP}",
    s")\n",
    s"""function join_by { local IFS="$$1"; shift; echo "$$*"; }""",
    s"""THE_CLASSPATH=$$(join_by : "$${JARS[@]}")""",
    s"java -cp $$THE_CLASSPATH $classname $$*"
  ).mkString("\n")

  val script_name = classname.split("\\.").last.toLowerCase.trim
  val script = new File(s"bash/${script_name}_java")
  script.setExecutable(true, true)
  val f = new FileWriter(script)
  f.write("#/usr/bin/bash \n\n")
  f.write(bash)
  f.write("\n")
  f.close
}
