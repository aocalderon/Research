import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PointRDD, CircleRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Coordinate, Point}
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import dbscan._

object ICPETester {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val precision: Double = 0.0001
  private var startApp: Long = System.currentTimeMillis()
  private var applicationID: String = "app-00000000000000-0000"

  case class Pids(t: Int, pids: List[Int])

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def round3(n: Double): Double = { math.round( n * 1000) / 1000.0 }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("ICPE|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startApp)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new ICPEConf(args)
    val debug = params.debug()
    val input = params.input()
    val output = params.output()
    val cores = params.cores()
    val executors = params.executors()
    val master = params.local() match {
      case true  => s"local[${cores}]"
      case false => s"spark://${params.host()}:${params.port()}"
    }
    val epsilon = params.epsilon()
    val mu = params.mu()
    val minpts = params.minpts()
    val width = params.width()

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
      .appName("ICPE")
      .getOrCreate()
    import spark.implicits._
    startApp = spark.sparkContext.startTime
    applicationID = spark.sparkContext.applicationId
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    val locations = spark.read
      .option("header", false)
      .option("delimiter", "\t")
      .csv(input).map{ row =>
        val id = row.getString(0).toInt
        val x  = round3(row.getString(1).toDouble)
        val y  = round3(row.getString(2).toDouble)
        val t  = row.getString(3).toInt

        ST_Point(id, x, y, t)
      }.cache()
    val nLocations = locations.count()
    log(stage, timer, nLocations, "END")

    val startGRIndex = clocktime
    val data = GRIndex.queryGrid(spark, GRIndex.allocateGrid(spark, locations, width, epsilon), epsilon).rdd
      .flatMap{ pair =>
        val p1 = pair._1
        val p2 = pair._2
        List(p1, p2)
      }.distinct().map{ p =>
        val x = p.x
        val y = p.y
        val da = new DoubleArray(Array(x, y))
        da.setTid(p.tid)
        da.setT(p.t)
        da
      }.collect().toList.asJava
    val endGRIndex = clocktime
    val algo = new AlgoDBSCAN()
    val clusters = algo.run(data, minpts, epsilon).asScala.toList
    val endTime = clocktime
    val dbscanTime = (algo.getTime() / 1000.0)
    val nClusters = clusters.size

    logger.info("ICPE|%s|%5.1f|%2d|%6.3f|%6.3f|%6d".format(applicationID, epsilon, mu, dbscanTime, ((endGRIndex - startGRIndex) / 1000.0), nClusters))

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}
