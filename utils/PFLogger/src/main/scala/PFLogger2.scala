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

object PFLogger2 {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class TimeByTime(timestamp: String, tag: String, appId: String, executors: Int,
    cores: Int, status: String, timer: Double, phase: String, phaseTime: Double,
    load: Int, timeInstant: Int)

  case class TimeByWindow(timestamp: String, tag: String, appId: String, executors: Int,
    cores: Int, status: String, timer: Double, phase: String, phaseTime: Double,
    load: Int, timeInstant: Int)

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
    implicit val params = new PFLoggerConf(args)
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

    val FF = timer{"Extracting FF data"}{
      val FF = lines.filter($"line".rlike("\\|FF\\|"))
        .filter($"line".rlike("\\|  END\\|")).cache

      val schema = ScalaReflection.schemaFor[TimeByTime].dataType.asInstanceOf[StructType]
      splitDF(FF, $"line", schema.fieldNames).cache()
    }

    val appParameters = Seq("ffpartitions", "mfpartitions")

    val apps = timer{"Extranting Apps data"}{
      val apps = lines.filter($"line".rlike("SparkSubmit")).rdd
        .flatMap{ row =>
          val line = row.getString(0)
          val arr = line.split("\\|")
          val appId = arr(2)
          val command = arr(3)

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
      val columnsA = List($"A.appId", $"phase", $"phaseTime".alias("time"), $"timeInstant".alias("instant"))
      val columnsB = (0 until appParameters.size).map(i => col("values").getItem(i).as(appParameters(i)))
      val columns = columnsA ++ columnsB

      FF.alias("A").join(apps.alias("B"), $"A.appId" === $"B.appId")
        .select( columns:_* ).cache
    }

    save{params.output()}{
      data.map{ d =>
        val appId = d.getString(0)
        val phase = d.getString(1)
        val time = d.getString(2)
        val instant = d.getString(3)
        val ffpartitions = d.getString(4)
        val mfpartitions = d.getString(5)

        s"${appId}\t${phase}\t${time}\t${instant}\t${ffpartitions}\t${mfpartitions}\n"
      }.collect()
    }

    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }
}

class PFLogger2Conf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](default = Some(""))
  val n = opt[Int](default = Some(50))
  val debug = opt[Boolean](default = Some(false))
  val output = opt[String](default = Some("/tmp/output.csv"))

  verify()
}
