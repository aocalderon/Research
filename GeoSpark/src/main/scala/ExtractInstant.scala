import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import org.rogach.scallop._

object ExtractInstant{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class Point(id: Long, x: Double, y: Double, t: Int)
  def main(args: Array[String]) = {
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

    logger.info("Reading data...")
    val ds = spark.read.option("header", "false").option("delimiter", "\t")
      .csv("/home/acald013/Datasets/LA/LA_clean.tsv")
      .map{ p =>
        val id = p.getString(0).toLong
        val x  = p.getString(1).toDouble
        val y  = p.getString(2).toDouble
        val t  = p.getString(3).toInt

        Point(id, x, y, t)
      }.cache()
    ds.show()
    logger.info(s"The total count is ${ds.count()}")

    val instant = 16
    logger.info(s"Extracting time instant $instant")
    val ds_filter = ds.filter($"t" === instant)
    ds_filter.show()
    logger.info(s"The count for time instant: ${ds_filter.count()}")

    val records = ds_filter.collect()
      .map(p => s"${p.id}\t${p.x}\t${p.y}\t${p.t}\n")
      .mkString("")
    val f = new java.io.PrintWriter(s"/home/acald013/Datasets/LA/LA_${instant}.tsv")
    f.write(records)
    f.close()
  }
}
