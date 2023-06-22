import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType}
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.geotools.referencing.CRS
import org.geotools.geometry.jts.JTS
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Coordinate, Point}
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import java.io.PrintWriter

object LATrajs {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val precision: Double = 0.0001
  private var startTime: Long = System.currentTimeMillis()

  case class ST_Point(pid: Long, x: Double, y: Double, t: Int)

  def round3(n: Double): Double = { math.round( n * 1000) / 1000 }

  def savePoints(points: RDD[ST_Point], filename: String): Unit = {
    val f = new PrintWriter(filename)
    f.write(points.map{ p =>
      var arr = p.x.toString().split("\\.")
      var integers = arr(0)
      var decimals = arr(1)
      if(decimals.size > 3) { decimals = decimals.substring(0, 3)}
      val x = s"${integers}.${decimals}"
      arr = p.y.toString().split("\\.")
      integers = arr(0)
      decimals = arr(1)
      if(decimals.size > 3) { decimals = decimals.substring(0, 3)}
      val y = s"${integers}.${decimals}"
      s"${p.pid}\t${x}\t${y}\t${p.t}\n"
    }.collect.mkString(""))
    f.close()
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("LATs|%6.2f|%-50s|%6.2f|%8d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new LATsConf(args)
    val input = params.input()
    val output = params.output()
    val offset = params.offset()
    val cores = params.cores()
    val executors = params.executors()
    val tstart = params.tstart()
    val tend = params.tend()
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
      .appName("LATrajs")
      .getOrCreate()
    import spark.implicits._
    startTime = spark.sparkContext.startTime
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    val points = spark.read.option("header", "false").option("delimiter", "\t").csv(input)
      .map{ p =>
        val pid = p.getString(0).toInt
        val x = p.getString(1).toDouble
        val y = p.getString(2).toDouble
        val t = p.getString(3).toInt

        ST_Point(pid, x, y, t)
      }.cache()
    val nPoints = points.count()
    log(stage, timer, nPoints, "END")

    //points.groupBy($"t").count().orderBy($"t").show(200, false)

    // Sample saved...
    timer = clocktime
    stage = "Sample saved"
    log(stage, timer, 0, "START")
    val sample = points.filter(p => tstart <= p.t && p.t <= tend).rdd.cache()
    savePoints(sample, output)
    val nSample = sample.count()
    log(stage, timer, nSample, "END")

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class LATsConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default  = Some("/tmp/output.tsv"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("KDBTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(1))
  val tstart:     ScallopOption[Int]     = opt[Int]     (default = Some(0))  
  val tend:       ScallopOption[Int]     = opt[Int]     (default = Some(200))  

  verify()
}
