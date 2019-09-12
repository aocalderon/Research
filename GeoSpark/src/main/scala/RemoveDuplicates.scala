import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import org.rogach.scallop._

object RemoveDuplicates{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class Point(id: Long, x: Double, y: Double, t: Int)
  def main(args: Array[String]) = {
    val params = new RDConf(args)
    val input = params.input()
    val output = params.output()

    // Starting session...
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 36)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.cores.max", 4 * 3)
      .config("spark.executor.cores", 4)
      .master("local[10]")
      .appName("RemoveDuplicates")
      .getOrCreate()
    import spark.implicits._

    val ds = spark.read.option("header", "false").option("delimiter", "\t")
      .csv(input)
      .map{ p =>
        val id = p.getString(0).toLong
        val x  = p.getString(1).toDouble
        val y  = p.getString(2).toDouble
        val t  = p.getString(3).toInt

        Point(id, x, y, t)
      }.cache()
    ds.show()
    logger.info(s"The initial count is ${ds.count()}")

    val ds_clean = ds.groupBy($"x",$"y",$"t").agg(min($"id").alias("id"))
    .select("id", "x", "y", "t").sort($"t").cache()
    ds_clean.show()
    logger.info(s"The new count is ${ds_clean.count()}")

    val records = ds_clean.collect()
      .map(p => s"${p.getLong(0)}\t${p.getDouble(1)}\t${p.getDouble(2)}\t${p.getInt(3)}\n")
      .mkString("")
    val f = new java.io.PrintWriter(output)
    f.write(records)
    f.close()
  }
}

import org.rogach.scallop._

class RDConf(args: Seq[String]) extends ScallopConf(args) {
  val input:        ScallopOption[String]  =  opt[String]   (default = Some(""))
  val output:       ScallopOption[String]  =  opt[String]   (default = Some(""))

  verify()
}

