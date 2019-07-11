import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import org.rogach.scallop._

object RemoveDuplicates{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

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

    val df = spark.read.option("header", "false").option("delimiter", "\t")
      .csv("/home/acald013/Research/Datasets/LA/LA_sample.tsv")
      .toDF("id", "x", "y", "t")
    df.show()
    logger.info(s"The initial count is ${df.count()}")

    val t10 = df.map{ d =>
      val t = d.getString(3)
      val coord = s"${d.getString(1)} ${d.getString(2)}"

      (t, coord)
    }.toDF("t", "coord").filter($"t" === 10)

    logger.info(s"Count: ${t10.select("coord").count()}")
    logger.info(s"Count without duplicates: ${t10.select("coord").distinct().count()}")
  }
}
