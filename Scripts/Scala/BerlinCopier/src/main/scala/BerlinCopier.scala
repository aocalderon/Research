import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

case class ST_Point(pid: Int, x: Double, y: Double, t: Int)

object BerlinCopier {
  private val logger : Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").getOrCreate()

    import spark.implicits._
    val filename = "/home/and/Documents/PhD/Research/Datasets/Berlin/berlin0-0.tsv"
    val data = spark.read.option("header", false)
      .option("delimiter", "\t").csv(filename)
      .map{ p =>
        (p.getString(0).toInt, p.getString(1).toDouble, p.getString(2).toDouble, p.getString(3).toInt)
      }.toDF("pid","x","y","t").as[ST_Point]

    val boundary = data.agg(min($"x"), min($"y"), max($"x"), max($"y")).collect()(0)
    val min_x = boundary.getDouble(0)
    val min_y = boundary.getDouble(1)
    val max_x = boundary.getDouble(2)
    val max_y = boundary.getDouble(3)

    logger.info(s"Extension: (${min_x} ${min_y}), (${max_x} ${max_y})")

    val extent_x = math.ceil(max_x - min_x).toInt
    val extent_y = math.ceil(max_y - min_y).toInt

    logger.info(s"Dimensions: $extent_x x $extent_y")
  }
}
