import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType}
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PointRDD}
import org.geotools.referencing.CRS
import org.geotools.geometry.jts.JTS
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Coordinate, Point, LineString}
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray
import java.io.PrintWriter

object GLTrajSplitter {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val precision: Double = 0.0001
  private var startTime: Long = System.currentTimeMillis()

  case class ST_Point(pid: Long, x: Double, y: Double, t: Long)

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("LATC|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new GLTrajSplitterConf(args)
    val input = params.input()
    val offset = params.offset()
    val cores = params.cores()
    val executors = params.executors()
    val partitions = params.partitions()
    val master = params.local() match {
      case true  => s"local[${cores}]"
      case false => s"spark://${params.host()}:${params.port()}"
    }

    var timer = clocktime
    var stage = "Session start"
    log(stage, timer, 0, "START")
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * cores * executors)
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.cores.max", cores * executors)
      .config("spark.executor.cores", cores)
      .master(master)
      .appName("LATrajSplitter")
      .getOrCreate()
    import spark.implicits._
    startTime = spark.sparkContext.startTime
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    var points = spark.read.option("header", "false").option("delimiter", "\t").csv(input)
      .map{ traj =>
        val pid = traj.getString(0).toLong
        val x = traj.getString(1).toDouble
        val y = traj.getString(2).toDouble
        val t = traj.getString(3).toInt
        ST_Point(pid, x, y, t)
      }.orderBy($"t").cache()
    val nTrajs = points.count()
    log(stage, timer, nTrajs, "END")

    timer = clocktime
    stage = "Saving time intervals"
    log(stage, timer, 0, "START")
    val ts = points.select($"t").distinct().collect().map(_.getLong(0)).sorted
    val nTs = ts.size
    logger.info(s"Total time intervals: ${nTs}")
    logger.info(s"Sample data: ${ts.take(5).mkString(" ")}")
    val index = new java.io.PrintWriter(s"${params.outdir()}${params.outname()}_index.tsv")
    for(t <- ts.take(params.n())){
      val sample = points.filter(_.t == t)
      val filename = s"${params.outdir()}${params.outname()}_${t}.tsv"
      val f = new java.io.PrintWriter(filename)
      f.write(sample.map(p => s"${p.pid}\t${p.x}\t${p.y}\t${p.t}\n").collect().mkString(""))
      f.close()
      logger.info(s"Saved: ${filename}")
      index.write(s"$t\n")
    }
    index.close()
    log(stage, timer, params.n(), "END")

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class GLTrajSplitterConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val outdir:     ScallopOption[String]  = opt[String]  (default  = Some("/tmp/"))
  val outname:    ScallopOption[String]  = opt[String]  (default  = Some("LA"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(256))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val n:          ScallopOption[Int]     = opt[Int]     (default = Some(10))
  val fraction:   ScallopOption[Double]  = opt[Double]  (default = Some(0.1))

  verify()
}
