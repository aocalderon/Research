name := "MaximalFinder"
version := "0.1.0"
organization := "edu.ucr.dblab"
scalaVersion := "2.11.11"
scalaVersion in ThisBuild := "2.11.11"

val SparkVersion = "2.3.0"
val SparkCompatibleVersion = "2.3"
val HadoopVersion = "2.7.2"
val GeoSparkVersion = "1.2.0"
val dependencyScope = "compile"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.spark" %% "spark-sql" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion % dependencyScope,
  "org.datasyslab" % "geospark" % GeoSparkVersion,
  "org.datasyslab" % "geospark-sql_".concat(SparkCompatibleVersion) % GeoSparkVersion,
  "org.datasyslab" % "JTSplus" % "0.1.4",
  "org.rogach" %% "scallop" % "3.1.5",
  "org.slf4j" % "slf4j-jdk14" % "1.7.25",
  "com.lihaoyi" %% "requests" % "0.1.7"
)

mainClass in (Compile, run) := Some("MF")
mainClass in (Compile, packageBin) := Some("MF")
