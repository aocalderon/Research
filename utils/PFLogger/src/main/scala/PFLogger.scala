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

object PFLogger {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class TimeByTime(timestamp: String, tag: String, status: String, appId: String,
  executors: Int, cores: Int, timer: Double, phase: String, phaseTime: Double, load: Int, timeInstant: Int)

case class TimeByWindow(timestamp: String, tag: String, status: String, appId: String,
  executors: Int, cores: Int, timer: Double, phase: String, phaseTime: Double, load: Int, timeInstant: Int)

  def debug[R](block: => R)(implicit d: Boolean): Unit = { if(d) block }

  def nrecords(msg: String, count: Long)(implicit debug: Boolean): Unit = {
    if(debug){ logger.info(s"$msg: $count") }
  }

  def timer[R](msg: String)(block: => R)(implicit logger: Logger): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    logger.info("%-30s|%6.2f".format(msg, (t1 - t0) / 1e9))
    result
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

  def setStartEndByColumn(dataframe: DataFrame, column: Column): DataFrame = {
    val names = dataframe.schema.fieldNames.filter(_ != column.toString)
    val df = dataframe
      .withColumn("start_1", min(column).over(Window.orderBy(column).rowsBetween(-1, -1)) + 1)
      .withColumn("start", when(col("start_1").isNull, 0).otherwise(col("start_1")))
      .withColumnRenamed(column.toString(), "end")
      .drop("start_1")

    val cols =  List(col("start"), col("end")) ++
    df.schema.fieldNames.filter(_ != "start").filter(_ != "end").map(col)

    df.select(cols: _*)
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
      val cores = conf.get("spark.executor.cores")
      val executors = conf.get("spark.executor.instances")
      val appId = conf.get("spark.app.id").split("-")(2)

      logger.info(s"AppId: $appId Cores: $cores Executors: $executors")
    }

    val lines = timer{"Reading data"}{
      val lines = spark.read.text(params.input())
        .toDF("line").cache
      nrecords("Lines", lines.count)
      lines
    }

    val FF = timer{"Extracting FF"}{
      val FF = lines.filter($"line".rlike("\\|FF\\|"))
        .filter($"line".rlike("\\|  END\\|")).cache
      nrecords("FF", FF.count)

      val schema = ScalaReflection.schemaFor[TimeByTime].dataType.asInstanceOf[StructType]
      splitDF(FF, $"line", schema.fieldNames)
    }

    val FE = timer{"Extracting FE"}{
      val FE = lines.filter($"line".rlike("\\|FE\\|"))
        .filter($"line".rlike("\\|END  \\|")).cache
      nrecords("FE", FE.count)

      val schema = ScalaReflection.schemaFor[TimeByWindow].dataType.asInstanceOf[StructType]
      splitDF(FE, $"line", schema.fieldNames)
    }

    val apps = timer{"Extranting apps info"}{
      val apps   = lines.filter($"line".rlike("spark.app.id")).collect()
        .map(_.getString(0).split(",").last.replace(")", "").split("-").last)
      val params = lines.filter($"line".rlike("spark-submit")).collect()
        .map(_.getString(0).split("--").filter(_.contains("epsilon")).head.split(" ").last.toDouble)

      spark.createDataFrame(apps.zip(params))
    }

    val data = timer{"Joining"}{
      val data = FF.union(FE)
        .select($"tag".alias("method"), $"timeInstant".alias("instant"), $"phaseTime".alias("time"), $"appId")
        .alias("A")
        .join(apps.toDF("appId", "epsilon").alias("B"), $"A.appId" === $"B.appId")
        .filter($"instant" !== 0)
        .groupBy($"method", $"epsilon", $"instant").agg(sum($"time").alias("time"))
        .groupBy($"method", $"epsilon").agg(avg($"time").alias("time"))
        .orderBy($"epsilon", $"method")
        .cache
      nrecords("data", data.count())
      data
    }

    timer{"Saving"}{
      val f = new java.io.PrintWriter(params.output())
      val content = data.map(d => f"${d.getString(0)}%s,${d.getDouble(1)}%.1f,${d.getDouble(2)}%.2f\n").collect()
      f.write(content.mkString(""))
      f.close()
      logger.info(s"Saved ${params.output()} [${content.size} records].")
    }

    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }
}

class PFLoggerConf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](default = Some(""))
  val n = opt[Int](default = Some(50))
  val debug = opt[Boolean](default = Some(false))
  val output = opt[String](default = Some("/tmp/output.csv"))

  verify()
}
