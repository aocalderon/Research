import org.apache.spark.storage.StorageLevel
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.datasyslab.geospark.enums.{GridType, IndexType, FileDataSplitter}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Coordinate}
import org.rogach.scallop._
import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConverters._
import java.io.PrintWriter

object Scaleup{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()

  def main(args: Array[String]) = {
    val params     = new ScaleupConf(args)
    val t1         = params.t1()
    val t2         = params.t2()
    val offset     = params.offset()
    val tag        = params.tag()
    val host       = params.host()
    val port       = params.port()
    val mfpartitions = params.mfpartitions()
    val ffpartitions = params.ffpartitions()
    val dpartitions  = params.dpartitions()
    val grid       = params.grid()
    val index      = params.index()
    val cores      = params.cores()
    val executors  = params.executors()
    val debug      = params.debug()
    val local      = params.local()
    var master     = ""
    if(local){
      master = s"local[$cores]"
    } else {
      master = s"spark://${host}:${port}"
    }
    val gridType = grid match {
      case "QUADTREE"  => GridType.QUADTREE
      case "RTREE"     => GridType.RTREE
      case "EQUALGRID" => GridType.EQUALGRID
      case "KDBTREE"   => GridType.KDBTREE
      case "HILBERT"   => GridType.HILBERT
      case "VORONOI"   => GridType.VORONOI
    }
    val indexType = index match {
      case "QUADTREE"  => IndexType.QUADTREE
      case "RTREE"     => IndexType.RTREE
    }

    // Starting session...
    var timer = clocktime
    log("Session start", timer, 0, "START")
    val maxCores = cores * executors
    val spark = SparkSession.builder().
      config("spark.serializer",classOf[KryoSerializer].getName).
      master(master).appName("Scaleup").
      config("spark.cores.max", maxCores).
      config("spark.executor.cores", cores).
      getOrCreate()
    import spark.implicits._
    val appID = spark.sparkContext.applicationId
    log("Session start", timer, 0, "END")    

    // Reading data...
    timer = clocktime
    log("T1 read", timer, 0, "START")
    val t1Points = new PointRDD(spark.sparkContext, t1, offset, FileDataSplitter.CSV, true, dpartitions * maxCores)
    val nT1 = t1Points.rawSpatialRDD.rdd.count()
    log("Data read", timer, nT1, "END")

    timer = clocktime
    log("T2 read", timer, 0, "START")
    val t2Points = new PointRDD(spark.sparkContext, t2, offset, FileDataSplitter.CSV, true, dpartitions * maxCores)
    val nT2 = t2Points.rawSpatialRDD.rdd.count()
    log("Data read", timer, nT2, "END")

    // Closing session...
    timer = clocktime
    log("Session close", timer, 0, "START")
    spark.close()
    log("Session close", timer, 0, "END")
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long = -1, status: String = "NULL"): Unit = {
    logger.info("%-50s|%6.2f|%6d|%s".format(msg, (clocktime - timer)/1000.0, n, status))
  }
}

class ScaleupConf(args: Seq[String]) extends ScallopConf(args) {
  val t1:     ScallopOption[String]  = opt[String]  (required = true)
  val t2:     ScallopOption[String]  = opt[String]  (required = true)
  val tag:        ScallopOption[String]  = opt[String]  (default = Some(""))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val mfpartitions: ScallopOption[Int]     = opt[Int]     (default = Some(256))
  val ffpartitions: ScallopOption[Int]     = opt[Int]     (default = Some(256))
  val dpartitions:  ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(1))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
