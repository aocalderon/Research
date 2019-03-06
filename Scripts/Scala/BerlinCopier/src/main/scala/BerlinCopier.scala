import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import scala.collection.JavaConverters._
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import java.io._

case class ST_Point(pid: Int, x: Double, y: Double, t: Int)

object BerlinCopier {
  private val logger : Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    val conf = new BCConf(args)
    val gap: Int = conf.gap()
    val eps: Double = conf.eps()
    val frame: Boolean = conf.frame()
    val indices: List[(Int, Int)] = List((1,0), (2,0), (3,0))
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    import spark.implicits._
    val filename = conf.input()
    val data = spark.read.option("header", false)
      .option("delimiter", "\t").csv(filename)
      .map{ p =>
        (p.getString(0).toInt, p.getString(1).toDouble, p.getString(2).toDouble, p.getString(3).toInt)
      }.toDF("pid","x","y","t").as[ST_Point]

    val nData = data.count()
    logger.info(s"Size of the original dataset: $nData points")

    val boundary = data.agg(min($"x"), min($"y"), max($"x"), max($"y")).collect()(0)
    val min_x = boundary.getDouble(0)
    val min_y = boundary.getDouble(1)
    val max_x = boundary.getDouble(2)
    val max_y = boundary.getDouble(3)

    logger.info(s"Extension ((min_x, min_y), (max_x, max_y)): (${"%.2f".format(min_x)} ${"%.2f".format(min_y)}), (${"%.2f".format(max_x)} ${"%.2f".format(max_y)})")

    val extent_x = max_x - min_x
    val extent_y = max_y - min_y

    logger.info(s"Dimensions: ${"%.2f".format(extent_x)} x ${"%.2f".format(extent_y)}")

    val max_pid = data.agg(max($"pid")).collect()(0).getInt(0) + 1

    logger.info(s"Max pid in this dataset: $max_pid")

    logger.info("Starting duplication...")
    var timer = System.currentTimeMillis()
    var duplication = spark.sparkContext.emptyRDD[ST_Point].toDS()
    val times = conf.times()
    if(times == 1){
      duplication = data
    } else {
      val n = times - 2
      duplication = (0 to n).par.map{ k =>
        val i = indices(k)._1
        val j = indices(k)._2

        data.map{ p =>
          val new_pid = p.pid + ((k + 1) * max_pid)
          val new_x   = p.x + (i * extent_x)
          val new_y   = p.y + (j * extent_y)
          ST_Point(new_pid, new_x, new_y, p.t)
        }
      }.reduce( (a, b) => a.union(b))
      .union(data)
      .sort($"pid", $"t").persist()
    }
    logger.info(s"Duplication done at ${(System.currentTimeMillis() - timer) / 1000.0}s")

    if(frame){
      val mus = List(0,1,2)
      val deltas = List(0,1,2,3,4,5)
      val corners = List((min_x - gap, min_y - gap), (min_x - gap, max_y + gap), (max_x + gap, max_y + gap), (max_x + gap, min_y - gap))
      val extension = corners.flatMap(corner => mus.flatMap(mu => deltas.map(delta => (corner, mu, delta))))
      val BoundaryGap = spark.sparkContext.parallelize{
        extension.map{ d =>
          ((d._1._1 + (d._2 * eps), d._1._2 + (d._2 * eps)), d._3)
        }.zipWithIndex.map{ d =>
          ST_Point(max_pid + d._2 + 1, d._1._1._1, d._1._1._2, d._1._2)
        }
      }.toDS()
      //BoundaryGap.show(72)
      duplication = BoundaryGap.union(duplication)
    }

    logger.info("Saving new dataset...")
    timer = System.currentTimeMillis()
    val nDataset = duplication.count()
    logger.info(s"Size of the new dataset: $nDataset points")
    val nboundary = duplication.agg(min($"x"), min($"y"), max($"x"), max($"y")).collect()(0)
    val nmin_x = nboundary.getDouble(0)
    val nmin_y = nboundary.getDouble(1)
    val nmax_x = nboundary.getDouble(2)
    val nmax_y = nboundary.getDouble(3)

    logger.info(s"New extension ((min_x, min_y), (max_x, max_y)): (${"%.2f".format(nmin_x)} ${"%.2f".format(nmin_y)}), (${"%.2f".format(nmax_x)} ${"%.2f".format(nmax_y)})")

    val nextent_x = nmax_x - nmin_x
    val nextent_y = nmax_y - nmin_y

    logger.info(s"New dimensions: ${"%.2f".format(nextent_x)} x ${"%.2f".format(nextent_y)}")

    val nmax_pid = duplication.agg(max($"pid")).collect()(0).getInt(0) + 1

    logger.info(s"New max pid in this dataset: $max_pid")

    if(conf.debug()){
      val out = duplication.map(p => (p.pid, p.t, s"${p.x} ${p.y}"))
        .toDF("pid", "t", "point").sort($"pid", $"t")
        .withColumn("traj", collect_list($"point").over(Window.partitionBy($"pid")))
        .map(t => (t.getInt(0), t.getList[String](3).asScala))
        .map(t => (t._1, s"LINESTRING(${t._2.mkString(",")})"))
        .toDF("pid", "traj").distinct().orderBy($"pid")

      out.show(false)
      val pw = new PrintWriter(new File("/tmp/output.wkt"))
      pw.write(out.map(t => s"${t.getInt(0)};${t.getString(1)}\n").collect().mkString(""))
      pw.close
    }
    val pw = new PrintWriter(new File(conf.output()))
    pw.write(duplication.map(p => s"${p.pid}\t${p.x}\t${p.y}\t${p.t}\n").collect().mkString(""))
    pw.close
    logger.info(s"Dataset saved at ${(System.currentTimeMillis() - timer) / 1000.0}s")

    spark.close()
  }
}

class BCConf(args: Seq[String]) extends ScallopConf(args) {
  val input:  ScallopOption[String]   = opt[String]  (required = true)
  val gap:    ScallopOption[Int]      = opt[Int]     (default  = Some(0))
  val eps:    ScallopOption[Double]   = opt[Double]  (default  = Some(0.25))
  val times:  ScallopOption[Int]      = opt[Int]     (default  = Some(2))
  val frame:  ScallopOption[Boolean]  = opt[Boolean] (default  = Some(false))
  val debug:  ScallopOption[Boolean]  = opt[Boolean] (default  = Some(false))
  val output: ScallopOption[String]   = opt[String]  (default  = Some("/tmp/output.tsv"))

  verify()
}
