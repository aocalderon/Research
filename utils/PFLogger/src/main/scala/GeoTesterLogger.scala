package org.dblab

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Column, Row}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.slf4j.{Logger, LoggerFactory}
import scala.annotation.tailrec
import org.rogach.scallop._

object GeoTesterLogger {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class Log(timestamp: String, tag: String, appId: String, phase: String, duration: Double)

  case class App(appId: String, partitions: Int, gridtype: String, indextype: String)

  case class Task(timestamp: String, tag: String, stage: String, ntasks: Int, stageId: Int,
    taskId: Int, index: Int, host: String, locality: String, duration: Double,
    rRead: Long, bRead: Long, rWritten: Long, bWritten: Long, rShuffleRead: Long, rShuffleWritten: Long,
    jobId: Int, appId: String, details: String)

  def clocktime = System.nanoTime()

  def debug[R](block: => R)(implicit d: Boolean): Unit = { if(d) block }

  def nrecords(msg: String, count: Long): Unit = {
    logger.info(s"$msg: $count") 
  }

  def timer[R](msg: String)(block: => R)(implicit logger: Logger): R = {
    val t0 = clocktime
    val result = block    // call-by-name
    val t1 = clocktime
    logger.info("%-30s|%6.2f".format(msg, (t1 - t0) / 1e9))
    result
  }

  def save(filename: String)(content: Seq[String]): Unit = {
    val t0 = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val t1 = clocktime
    val time = (t1 - t0) / 1e9
    logger.info(f"Saved ${filename} in ${time}%.2fs [${content.size} records].")
  }  

  def splitDF(df: DataFrame, column: Column, columns: Array[String], delimiter: String = "\\|"):
      DataFrame = {
    df.withColumn("temp", split(column, delimiter))
      .select{
        (0 until columns.size).map(i => col("temp").getItem(i).as(columns(i))): _*
      }
  }

  @tailrec
  def loopFields(fields: List[StructField], df: DataFrame): DataFrame = {
    fields match {
      case Nil => df
      case head +: tail => {
        val name = head.name
        val dataType = head.dataType
        val df2 = df.withColumn(name + "_1", trim(df(name)).cast(dataType))
          .drop(name)
          .withColumnRenamed(name + "_1", name)
        loopFields(tail, df2)
      }
    }
  }

  def setSchema(dataframe: DataFrame, schema: StructType): DataFrame = {
    loopFields(schema.fields.toList, dataframe)
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting session...")
    implicit val params = new GeoTesterLoggerConf(args)
    implicit val spark = SparkSession.builder().getOrCreate
    implicit val conf = spark.sparkContext.getConf
    import spark.implicits._
    implicit val debugOn = params.debug()
    logger.info("Starting session... Done!")

    debug{
      val master = conf.get("spark.master")
      val (cores, executors, appId) = if(master.contains("local")){
        val cores = master.split("\\[")(1).replace("]", "").toInt
        val executors = 1
        val appId = conf.get("spark.app.id")
        (cores, executors, appId)
      } else {
        val cores = conf.get("spark.executor.cores")
        val executors = conf.get("spark.executor.instances")
        val appId = conf.get("spark.app.id").takeRight(4)
        (cores, executors, appId)
      }
      logger.info(s"AppId: $appId Cores: $cores Executors: $executors")
    }

    val lines = timer{"Reading data"}{
      val lines = spark.read.text(params.input())
        .toDF("line").cache
      nrecords("Lines", lines.count)
      lines
    }

    val log = timer{"Extracting GeoTester data"}{
      val data = lines.filter($"line".rlike("\\|GeoTesterRDD\\|")).cache

      val schema = ScalaReflection.schemaFor[Log].dataType.asInstanceOf[StructType]
      splitDF(data, $"line", schema.fieldNames).cache()
    }

    val tasks = timer{"Extracting Tasks data"}{
      val data = lines.filter($"line".rlike("\\|TASKINFO\\|")).cache

      val schema = ScalaReflection.schemaFor[Task].dataType.asInstanceOf[StructType]
      splitDF(data, $"line", schema.fieldNames).cache()
    }

    save{"/home/acald013/Research/tmp/GeoTesterRDD_tasks.txt"}{
      tasks.select($"stageId",$"stage",$"ntasks",$"taskId",$"host",$"locality",$"duration", $"rRead", $"bRead", $"rWritten", $"bWritten", $"rShuffleRead", $"rShuffleWritten", $"jobId",$"appId")
        .map{ t =>
          s"${t.getString(0)}\t" +
          s"${t.getString(1)}\t" +
          s"${t.getString(2)}\t" +
          s"${t.getString(3)}\t" +
          s"${t.getString(4)}\t" +
          s"${t.getString(5)}\t" +
          s"${t.getString(6)}\t" +
          s"${t.getString(7)}\t" +
          s"${t.getString(8)}\t" +
          s"${t.getString(9)}\t" +
          s"${t.getString(10)}\t" +
          s"${t.getString(11)}\t" +
          s"${t.getString(12)}\t" +
          s"${t.getString(13)}\t" +
          s"${t.getString(14)}\n" 
        }
        .collect()          
    }

    val appParameters = Seq("partitions", "gridtype", "indextype")

    val apps = timer{"Extranting Apps data"}{
      val apps = lines.filter($"line".rlike("SparkSubmit")).rdd
        .flatMap{ row =>
          val line = row.getString(0)
          val arr = line.split("\\|")
          val appId = arr(1)
          val command = arr(2)

          command.split("--").map{ param => (appId, param) }
            .filter(_._2.split(" ").size == 2).map{ case(appId, param) =>
              val arr = param.split(" ")
              (appId, arr(0), arr(1))
            }
        }

      apps.toDF("appId", "param", "value")
        .filter($"param".isin(appParameters:_*))
        .orderBy($"appId", $"param")
        .groupBy($"appId")
        .agg(collect_list($"param").alias("params"), collect_list($"value").alias("values"))
        .orderBy($"appId").cache
    }

    val data = timer{"Joining"}{
      val columnsA = List($"A.appId", $"phase", $"duration")
      val columnsB = (0 until appParameters.size).map(i => col("values").getItem(i).as(appParameters(i)))
      val columns = columnsA ++ columnsB

      log.alias("A").join(apps.alias("B"), $"A.appId" === $"B.appId")
        .select( columns:_* ).cache
    }

    save{params.output()}{
      data.map{ d =>
        val appId = d.getString(0)
        val phase = d.getString(1)
        val time = d.getString(2)
        val instant = d.getString(3)
        val mfpartitions = d.getString(4)
        val dpartitions = d.getString(5)

        s"${appId}\t${phase}\t${time}\t${instant}\t${mfpartitions}\t${dpartitions}\n"
      }.collect()
    }

    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }
}

class GeoTesterLoggerConf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](default = Some(""))
  val n = opt[Int](default = Some(50))
  val debug = opt[Boolean](default = Some(false))
  val output = opt[String](default = Some("/tmp/output.csv"))

  verify()
}
