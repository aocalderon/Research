import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import java.io.PrintWriter

object LATrajIndexer {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  def timer[R](msg: String = "")(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    logger.info("LATI|%-30s|%6.2f".format(msg, (t1 - t0) / 1e9))
    result
  }

  def main(args: Array[String]): Unit = {
    val params = new LATIConf(args)
    val input = params.input()
    val output = params.output()
    
    val spark = timer("Starting session"){
      SparkSession.builder()
        .appName("LATrajIndexer")
        .getOrCreate()
    }
    import spark.implicits._

    val (trajs, nTrajs) = timer("Getting traj lengths"){
      val points = spark.read.option("header", "false").option("delimiter", "\t").csv(input)
        .map{ _.getString(0).toInt}.cache()
      val trajs = points.groupBy($"value").count()
      val nTrajs = trajs.count()
      (trajs, nTrajs)
    }

    timer("Saving index"){
      val lines = trajs.map{ row =>
        val tid = row.getInt(0)
        val n = row.getLong(1)
        s"$tid\t$n\n"
      }.collect()
      val f = new PrintWriter(output)
      f.write(lines.mkString)
      f.close
      logger.info(s"$output saved [${lines.size} records].")
    }

    timer("Closing session"){
      spark.close()
    }
  }
}

class LATIConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default  = Some("/tmp/output.tsv"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val tstart:     ScallopOption[Int]     = opt[Int]     (default = Some(0))  
  val tend:       ScallopOption[Int]     = opt[Int]     (default = Some(200))  

  verify()
}
