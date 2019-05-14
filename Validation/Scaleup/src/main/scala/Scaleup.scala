import org.apache.spark.storage.StorageLevel
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.spatialRDD.{PointRDD, CircleRDD}
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
    val output     = params.output()
    val distance   = params.distance() + params.precision()
    val offset     = params.offset()
    val tag        = params.tag()
    val host       = params.host()
    val port       = params.port()
    val mfpartitions = params.mfpartitions()
    val ffpartitions = params.ffpartitions()
    val dpartitions  = params.dpartitions()
    val mfcustomx    = params.mfcustomx()
    val mfcustomy    = params.mfcustomy()
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
      case "CUSTOM"    => GridType.CUSTOM
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
    val startTime = spark.sparkContext.startTime
    val defaultPartitions = dpartitions * maxCores
    log("Session start", timer, 0, "END")    

    // Reading data...
    timer = clocktime
    log("T1 read", timer, 0, "START")
    val t1Points = new PointRDD(spark.sparkContext, t1, offset, FileDataSplitter.CSV, true, defaultPartitions)
    val nT1 = t1Points.rawSpatialRDD.rdd.count()
    log("T1 read", timer, nT1, "END")

    timer = clocktime
    log("T2 read", timer, 0, "START")
    val t2Points = new PointRDD(spark.sparkContext, t2, offset, FileDataSplitter.CSV, true, defaultPartitions)
    val nT2 = t2Points.rawSpatialRDD.rdd.count()
    log("T2 read", timer, nT2, "END")

    // Spatial join...
    timer = clocktime
    log("Spatial join", timer, 0, "START")
    t1Points.analyze()
    t1Points.setNumX(mfcustomx)
    t1Points.setNumY(mfcustomy)
    t1Points.spatialPartitioning(gridType, defaultPartitions)
    t1Points.buildIndex(indexType, true)
    val buffer = new CircleRDD(t2Points, distance)
    buffer.spatialPartitioning(t1Points.getPartitioner)
    val R = JoinQuery.DistanceJoinQueryFlat(t1Points, buffer, true, false)
    val nR = R.rdd.count()
    log("Spatial join", timer, nR, "END")

    // Closing session...
    timer = clocktime
    log("Session close", timer, 0, "START")
    InfoTracker.master = host
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

class ScaleupConf(args: Seq[String]) extends ScallopConf(args) {
  val t1:     ScallopOption[String]  = opt[String]  (required = true)
  val t2:     ScallopOption[String]  = opt[String]  (required = true)
  val output: ScallopOption[String]  = opt[String]  (default = Some("/home/acald013/Research/Validation/Scaleup/logs"))
  val tag:        ScallopOption[String]  = opt[String]  (default = Some(""))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val distance:   ScallopOption[Double]  = opt[Double]  (default = Some(110.0))
  val precision:  ScallopOption[Double]  = opt[Double]  (default = Some(0.0001))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val mfpartitions: ScallopOption[Int]     = opt[Int]     (default = Some(256))
  val ffpartitions: ScallopOption[Int]     = opt[Int]     (default = Some(256))
  val dpartitions:  ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val mfcustomx:    ScallopOption[Int]     = opt[Int]     (default = Some(30))
  val mfcustomy:    ScallopOption[Int]     = opt[Int]     (default = Some(30))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(1))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
