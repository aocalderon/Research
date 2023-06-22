import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}

object DatasetSplitter {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  case class Point(id: Int, x: Double, y: Double, t: Int)

  def main(args: Array[String]): Unit = {
    val params = new DSConf(args)
    val input = params.input()
    val delimiter = params.delimiter()
    val output = if(params.output().isEmpty()) {
      input.split("/").dropRight(1).mkString("/")
    } else {
      params.output()
    }
    val (tag, ext) = {
      val filename = input.split("/").takeRight(1).head.split("\\.")
      (filename(0), filename(1))
    }

    val spark = SparkSession.builder()
      .appName("DatasetSplitter")
      .getOrCreate()
    import spark.implicits._
    logger.info("Session started")

    val data = spark.read.option("delimiter", delimiter).csv(input)
      .map{ line =>
        val id = line.getString(0).toInt
        val x = line.getString(1).toDouble
        val y = line.getString(2).toDouble
        val t = line.getString(3).toInt

        Point(id, x, y, t)
      }.cache
    val nData = data.count()
    logger.info(s"Number of records: $nData")

    val timestamps = data.rdd.map(_.t).distinct().collect().sorted
    logger.info(s"Number of timestamps: ${timestamps.size}")

    timestamps.foreach { t =>
      val text = data.filter(_.t == t).map{ d =>
        s"${d.id}\t${d.x}\t${d.y}\t${d.t}\n"
      }.collect()
      val filename = s"${output}/${tag}_${t}.${ext}"
      val f = new java.io.PrintWriter(filename)
      f.write(text.mkString(""))
      f.close
      logger.info(s"Saved $filename [${text.size} records].")
    }

    spark.close()
    logger.info("Session closed")
  }
}

class DSConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String]  (required = true)
  val output: ScallopOption[String] = opt[String]  (default = Some(""))
  val delimiter: ScallopOption[String] = opt[String]  (default = Some("\t"))
  val debug: ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
