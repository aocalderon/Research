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

object LATrajMerger {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val precision: Double = 0.0001
  private var startTime: Long = System.currentTimeMillis()

  case class Traj(index: Long, tid: Long, lenght: Int, t: Int = -1)

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("LATC|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new LATrajMergerConf(args)
    val input = params.input()
    val indexFile = params.index()
    val output = params.output()
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
      //.config("spark.cores.max", cores * executors)
      //.config("spark.executor.cores", cores)
      //.master(master)
      .appName("LATrajMerger")
      .getOrCreate()
    import spark.implicits._
    startTime = spark.sparkContext.startTime
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    var trajs = spark.read.option("header", "false").option("delimiter", "\t").csv(input)
      .map{ traj =>
        val tid = traj.getString(0).toLong
        tid
      }.cache()
    val nTrajs = trajs.count()
    log(stage, timer, nTrajs, "END")

    timer = clocktime
    stage = "Index read"
    log(stage, timer, 0, "START")
    var index = spark.read.option("header", "false").option("delimiter", "\t").csv(indexFile)
      .map{ index =>
        val t = index.getString(0).toInt
        val i = index.getString(1).toInt
        (t, i)
      }.flatMap(i => List.fill(i._2)(i._1)).cache()
    val nIndex = index.count()
    log(stage, timer, nIndex, "END")

    timer = clocktime
    stage = "Saving data"
    log(stage, timer, 0, "START")
    val table = index.collect().zip(trajs.collect()).map(r => s"${r._1}\t${r._2}\n")
    val f = new java.io.PrintWriter(params.output())
    f.write(table.mkString(""))
    f.close()
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class LATrajMergerConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default  = Some("/tmp/output.tsv"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("/home/acald013/Research/tmp/traj_index.txt"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(256))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val m:          ScallopOption[Int]     = opt[Int]     (default = Some(50000))
  val fraction:   ScallopOption[Double]  = opt[Double]  (default = Some(0.1))

  verify()
}
