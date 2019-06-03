import org.apache.spark.storage.StorageLevel
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.spatialRDD.{PointRDD, CircleRDD}
import org.datasyslab.geospark.enums.{GridType, IndexType, FileDataSplitter}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Coordinate}
import org.rogach.scallop._
import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConverters._
import java.io.PrintWriter

object SelfJoin{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()

  def main(args: Array[String]) = {
    val params     = new SJConf(args)
    val input      = params.input()
    val output     = params.output()
    val distance   = params.distance() + params.precision()
    val offset     = params.offset()
    val host       = params.host()
    val port       = params.port()
    val portUI     = params.portui()
    val customx    = params.customx()
    val customy    = params.customy()
    val grid       = params.grid()
    val index      = params.index()
    val cores      = params.cores()
    val executors  = params.executors()
    val debug      = params.debug()
    val local      = params.local()
    val gridType = grid match {
      case "QUADTREE"  => GridType.QUADTREE
      case "RTREE"     => GridType.RTREE
      case "EQUALGRID" => GridType.EQUALGRID
      case "KDBTREE"   => GridType.KDBTREE
      case "HILBERT"   => GridType.HILBERT
      case "VORONOI"   => GridType.VORONOI
      case "CUSTOM"    => GridType.CUSTOM
    }
    val indexType = index match {
      case "QUADTREE"  => IndexType.QUADTREE
      case "RTREE"     => IndexType.RTREE
    }
    var partitions = params.partitions()
    var master     = ""
    if(local){
      master = s"local[$cores]"
    } else {
      master = s"spark://${host}:${port}"
    }

    // Starting session...
    var timer = clocktime
    log("Session start", timer, 0, "START")
    val maxCores = cores * executors
    val spark = SparkSession.builder().
      config("spark.default.parallelism", 3 * cores * executors).
      config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      config("spark.scheduler.mode", "FAIR").
      master(master).appName("Scaleup").
      config("spark.cores.max", maxCores).
      config("spark.executor.cores", cores).
      getOrCreate()
    import spark.implicits._
    val appID = spark.sparkContext.applicationId
    val startTime = spark.sparkContext.startTime
    log("Session start", timer, 0, "END")    

    // Reading data...
    timer = clocktime
    log("Input read", timer, 0, "START")
    val points = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true)
    val nPoints = points.rawSpatialRDD.rdd.count()
    log("Input read", timer, nPoints, "END")

    // Spatial self-join...
    timer = clocktime
    log("Spatial self-join", timer, 0, "START")
    points.analyze()
    if(grid == "CUSTOM"){
      points.setNumX(customx.toInt)
      points.setNumY(customy.toInt)
      partitions = (customx * customy).toInt
    }
    points.spatialPartitioning(gridType, partitions)
    points.buildIndex(indexType, true)
    val buffer = new CircleRDD(points, distance)
    buffer.spatialPartitioning(points.getPartitioner)
    val R = JoinQuery.DistanceJoinQueryFlat(points, buffer, true, false)
    val nR = R.rdd.count()
    log("Spatial self-join", timer, nR, "END")

    // Closing session...
    timer = clocktime
    log("Session close", timer, 0, "START")
    InfoTracker.master = host
    InfoTracker.port = portUI
    InfoTracker.applicationID = appID
    InfoTracker.executors = executors
    InfoTracker.cores = cores
    val app_count = appID.split("-").reverse.head
    val f = new java.io.PrintWriter(s"${output}/app-${app_count}_info.tsv")
    f.write(InfoTracker.getExectutorsInfo())
    f.write(InfoTracker.getStagesInfo())
    f.write(InfoTracker.getTasksInfo())
    f.close()
    spark.close()
    log("Session close", timer, 0, "END")
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long = -1, status: String = "NULL"): Unit = {
    logger.info("%-50s|%6.2f|%6d|%s".format(msg, (clocktime - timer)/1000.0, n, status))
  }
}

class SJConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default = Some("/home/acald013/Research/Validation/Scaleup/logs"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val portui:     ScallopOption[String]  = opt[String]  (default = Some("4040"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val distance:   ScallopOption[Double]  = opt[Double]  (default = Some(110.0))
  val precision:  ScallopOption[Double]  = opt[Double]  (default = Some(0.001))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions:  ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val customx:    ScallopOption[Int]     = opt[Int]     (default = Some(30))
  val customy:    ScallopOption[Int]     = opt[Int]     (default = Some(30))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(1))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
