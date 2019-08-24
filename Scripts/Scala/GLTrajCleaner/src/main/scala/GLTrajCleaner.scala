import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.{min, max}
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
import scala.io.Source

object GLTrajCleaner {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val precision: Double = 0.0001
  private var startTime: Long = System.currentTimeMillis()

  case class ST_Row(pid: Long, x: Double, y: Double, date: String)
  case class ST_Point(pid: Long, x: Double, y: Double, t: Long)

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
    logger.info("LATC|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new GLTrajCleanerConf(args)
    val input = params.input()
    val output = params.output()
    val offset = params.offset()
    val cores = params.cores()
    val executors = params.executors()
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
      .config("spark.kryoserializer.buffer.max.mb", "1024")
      .master(master)
      .appName("LATrajCleaner")
      .getOrCreate()
    import spark.implicits._
    startTime = spark.sparkContext.startTime
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    val rows = spark.read.textFile(input)
      .rdd.zipWithUniqueId().flatMap{ f =>
        val filename = f._1
        val tid = f._2
        logger.info(s"Reading file $filename ...")
        val file = Source.fromFile(filename)
        val lines = file.getLines.toList.drop(6)
        val rows = lines.map{ line =>
          val p = line.split(",")
          val x = p(0).toDouble
          val y = p(1).toDouble
          val t = s"${p(5)} ${p(6)}"

          ST_Row(tid, x, y, t)
        }
        file.close()
        rows
      }.toDS().cache()
    val nRows = rows.count()
    log(stage, timer, nRows, "END")

    rows.show(false)

    timer = clocktime
    stage = "Coordinate transformation"
    log(stage, timer, 0, "START")
    val points = rows.withColumn("t", $"date".cast("timestamp").cast("long")).map{ r =>
      val pid = r.getLong(0)
      val x = r.getDouble(1)
      val y = r.getDouble(2)
      val t = r.getLong(4)
      ST_Point(pid, x, y, t)
    }
    val sourceCRS = CRS.decode("EPSG:4326")
    val targetCRS = CRS.decode("EPSG:4799")
    val lenient = true; // allow for some error due to different datums
    val transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
    val points2 = points.map{ p =>
      val sourcePoint = geofactory.createPoint(new Coordinate(p.x, p.y))
      val targetPoint = JTS.transform(sourcePoint, transform).asInstanceOf[Point]

      ST_Point(p.pid, targetPoint.getX, targetPoint.getY, p.t)
    }.repartition(120).cache()
    val nPoints2 = points2.count()
    log(stage, timer, nPoints2, "END")

    points2.show(false)
    points2.rdd.map(p => s"${p.pid}\t${p.x}\t${p.y}\t${p.t}").saveAsTextFile(output)
    val ts = points2.select($"t").distinct()
    logger.info(s"# of time instants: ${ts.count()}")
    logger.info(s"Min time instant: ${ts.agg(min($"t")).collect().head.getLong(0)}")
    logger.info(s"Max time instant: ${ts.agg(max($"t")).collect().head.getLong(0)}")

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class GLTrajCleanerConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default  = Some("/tmp/output"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("KDBTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(2))

  verify()
}
