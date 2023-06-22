import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions._
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

object GLTrajInterpolator {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val precision: Double = 0.0001
  private var startTime: Long = System.currentTimeMillis()

  case class ST_Point(point: Point, t: Long){
    val x = round3(point.getX)
    val y = round3(point.getY)

    override def toString(): String = s"($x $y $t)"
  }

  case class Traj(tid: Long, points: List[ST_Point]){
    val size = points.size

    override def toString(): String = s"${tid}\t[${size}]:\t${points.take(3).mkString(" ")}..."
  }

  def round3(n: Double): Double = { math.round( n * 1000) / 1000.0 }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("LATC|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new GLTrajInterpolatorConf(args)
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
      .appName("GLTraj")
      .getOrCreate()
    import spark.implicits._
    startTime = spark.sparkContext.startTime
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    val rows = spark.read
      .option("header", false)
      .option("delimiter", "\t")
      .csv(input)
      .repartition(params.partitions())
      .cache()
    log(stage, timer, 0, "END")

    /*
    timer = clocktime
    stage = "Points per time instant"
    log(stage, timer, 0, "START")
    val points = rows.map{ row =>
      val tid = row.getString(0).toLong
      val t = row.getString(3).toLong
      (t, tid)
    }.toDF("t", "tid").groupBy($"t").count().cache()
    points.orderBy($"count".desc).show(false)
    log(stage, timer, 0, "END")
     */
    
    timer = clocktime
    stage = "Longest trajectory"
    log(stage, timer, 0, "START")
    val trajs = rows.rdd.map{ row =>
        val tid = row.getString(0).toLong
        val x = row.getString(1).toDouble
        val y = row.getString(2).toDouble
        val coord = new Coordinate(x, y)
        val p = geofactory.createPoint(coord)
        val t = row.getString(3).toLong
        val point = ST_Point(p, t)
        (tid, List(point))
      }
      .reduceByKey( (a, b) => a ++ b, params.partitions())
      .map( t => Traj(t._1, t._2.sortBy(p => p.t))).cache()
    val nTrajs = trajs.count()
    log(stage, timer, nTrajs, "END")

    timer = clocktime
    stage = "Interpolation"
    log(stage, timer, 0, "START")
    val toInterpolate = trajs.flatMap{ traj =>
      val pairs = traj.points.zip(traj.points.tail)
      pairs.map{ pair =>
        val tid = traj.tid
        val diff = pair._2.t - pair._1.t

        (tid, pair._1, pair._2, diff)
      }
    }.filter(_._4 > 1)
      .map{ m =>
        val tid = m._1
        val p1  = s"${m._2.x} ${m._2.y} ${m._2.t}"
        val p2  = s"${m._3.x} ${m._3.y} ${m._3.t}"
        val diff = m._4
        s"$tid\t$p1\t$p2\t$diff\n"
      }.cache()
    val nToInterpolate = toInterpolate.count()
    val f = new java.io.PrintWriter("/tmp/toInterpolate.tsv")
    f.write(toInterpolate.collect().mkString(""))
    f.close()
    log(stage, timer, nToInterpolate, "END")

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class GLTrajInterpolatorConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default  = Some("/tmp/output2"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(8))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("KDBTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(2))

  verify()
}
