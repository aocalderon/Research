
import $exclude.`org.slf4j:slf4j-log4j12`, $ivy.`org.slf4j:slf4j-nop:1.7.21` // for cleaner logs
import $ivy.`org.apache.spark::spark-sql:2.1.0` // adjust spark version - spark >= 2.0
import $ivy.`org.jupyter-scala::spark:0.4.2` // for JupyterSparkSession (SparkSession aware of the jupyter-scala kernel)


import org.apache.spark._
import org.apache.spark.sql._
import jupyter.spark.session._


val spark = JupyterSparkSession.builder() // important - call this rather than SparkSession.builder()
  .jupyter() // this method must be called straightaway after builder()
  .master("local[*]") // change to "yarn-client" on YARN
  .config("spark.executor.memory", "6g")
  .appName("NohupReader")
  .getOrCreate()

val research_home: String = scala.util.Properties.envOrElse("RESEARCH_HOME", "/home/and/Documents/PhD/Research/")
val filename = s"${research_home}Scripts/Python/test.out"
val nohup = spark.read.textFile(filename)

org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
case class Line(line: String, n: Long)
case class Param(runID: Long, date: String, method: String, cores: Int, epsilon: Double, mu: Int, delta: Int, methodTime: Double)
case class Stat(runID: Long, n: Long, timestamp: String, stage: String, stageTime: Double, load: Int, unit: String)
case class Record(runID: Long, date: String, method: String, cores: Int, epsilon: Double, mu: Int, delta: Int, methodTime: Double, n: Long, timestamp: String, stage: String, stageTime: Double, load: Int, unit: String)

import org.apache.spark.sql.functions._
import spark.implicits._

val lines = nohup.toDF("line").withColumn("n", monotonicallyIncreasingId).as[Line].cache()
val nLines = lines.count()

val indices = lines.filter{ l =>
        l.line.contains("=== MergeLast Start ===") || l.line.contains("method=MergeLast,")
    }
    .orderBy("n")
    .select("n")
    .collect()
    .toList
    .map(_.getLong(0))
    .grouped(2)
    .toList
    .map(pair => (pair.head, pair.last))
    .filter(r => r._1 != r._2)
    .zipWithIndex
val index = spark.createDataset(indices)
    .flatMap{ pair =>
        (pair._1._1 to pair._1._2)
        .toList.map(v => (pair._2, v))
    }
    .toDF("runID","n")
    .cache

val runs = index.join(lines, "n").cache

val params = runs.groupBy("runID")
    .agg(max($"n").alias("n"))
    .join(lines, "n")
    .select("runID", "line")
    .orderBy("runID")
    .map{ row =>
        val runID = row.getInt(0)
        val line  = row.getString(1)
        var arr1  = line.split(" -> ")
        val date  = arr1(0)
        val arr2  = arr1(1).split(",")
        val method  = arr2(0).split("=")(1)
        val cores   = arr2(1).split("=")(1).toInt
        val epsilon = arr2(2).split("=")(1).toDouble
        val mu      = arr2(3).split("=")(1).toInt
        val delta   = arr2(4).split("=")(1).toInt
        val time    = arr2(5).split("=")(1).toDouble
        Param(runID, date, method, cores, epsilon, mu, delta, time)
    }
    .cache
params.show(10, truncate = false)

val stats = runs.filter(_.getString(2).contains("|"))
    .map{ row =>
        val n     = row.getLong(0)
        val runID = row.getInt(1)
        val line  = row.getString(2)
        var arr1  = line.split(" -> ")
        val timestamp  = arr1(0).trim
        val arr2  = arr1(1).split("\\|")
        val stage = arr2(0).trim
        val time  = arr2(1).trim.dropRight(1).toDouble
        val arr3  = arr2(2).trim.split(" ")
        val load  = arr3(0).toInt
        val unit  = arr3(1)
        Stat(runID, n, timestamp, stage, time, load, unit)
    }.cache
stats.count()
stats.show(10, truncate=false)

val data = params.join(stats, "runID").orderBy("n").cache
data.count()
data.show(10)

val d = data.collect.sortBy(_.getLong(8)).map(_.mkString(";")).mkString("\n")
val path = "Experiments/Logs/"

import java.io._
val pw = new PrintWriter(new File(s"${research_home}${path}output.csv" ))
pw.write(s"$d\n")
pw.close

data.as[Record]
  .cube("method", "stage")
  .agg(avg("stageTime").as("time"))
  .sort($"method".desc_nulls_last, $"stage".asc_nulls_last)
  .show
spark.close()
