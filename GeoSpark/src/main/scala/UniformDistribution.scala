import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialPartitioning.GridPartitioner
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PolygonRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialPartitioning.{KDBTree, KDBTreePartitioner}
import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.operation.buffer.BufferParameters
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Envelope, Coordinate, Polygon, LinearRing, Point}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import SPMF.{AlgoLCM2, Transactions, Transaction}
import org.rogach.scallop._

object UniformDistribution{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val reader = new com.vividsolutions.jts.io.WKTReader(geofactory)
  private val precision: Double = 0.001
  private var tag: String = ""
  private var appID: String = "app-00000000000000-0000"
  private var startTime: Long = clocktime
  private var cores: Int = 0
  private var executors: Int = 0

  case class ST_Point(id: Long, x: Double, y: Double, t: Int)

  def clocktime = System.currentTimeMillis()

  def logEnd(msg: String, timer: Long, n: Long): Unit ={
    val duration = (clocktime - startTime) / 1000.0
    logger.info("MF|%-30s|%6.2f|%-50s|%6.2f|%6d|%s".format(s"$appID|$executors|$cores|  END", duration, msg, (System.currentTimeMillis()-timer)/1000.0, n, tag))
  }

  def logStart(msg: String): Unit ={
    val duration = (clocktime - startTime) / 1000.0
    logger.info("MF|%-30s|%6.2f|%-50s|%6.2f|%6d|%s".format(s"$appID|$executors|$cores|START", duration, msg, 0.0, 0, tag))
  }

  def roundAt(p: Int)(n: Double): Double = { val s = math pow (10, p); (math round n * s) / s }

  /***
   * The main function...
   **/
  def main(args: Array[String]) = {
    val params: FFConf = new FFConf(args)
    val input       = params.input()
    val p_grid      = params.p_grid()
    val m_grid      = params.m_grid()
    val host        = params.host()
    val port        = params.port()
    val portUI      = params.portui()
    val offset      = params.offset()
    val sepsg       = params.sespg()
    val tepsg       = params.tespg()
    val info        = params.info()
    val timestamp   = params.timestamp()
    val epsilon     = params.epsilon()
    cores           = params.cores()
    executors       = params.executors()
    val master      = params.local() match {
      case true  => s"local[${cores}]"
      case false => s"spark://${host}:${port}"
    }
    val Dpartitions = (cores * executors) * params.dpartitions()
    val Mpartitions = params.mfpartitions()

    // Starting session...
    var timer = clocktime
    var stage = "Session started"
    logStart(stage)
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * cores * executors)
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.cores.max", cores * executors)
      .config("spark.executor.cores", cores)
      .master(master)
      .appName("MaximalFinder")
      .getOrCreate()
    import spark.implicits._
    appID = spark.sparkContext.applicationId
    startTime = spark.sparkContext.startTime
    logEnd(stage, timer, 0)

    // Reading data...
    timer = System.currentTimeMillis()
    stage = "Data read"
    logStart(stage)
    var points = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true, Dpartitions)
    if(timestamp >= 0){
      points = new PointRDD(points.rawSpatialRDD.rdd.filter{p =>
        val arr = p.getUserData.toString().split("\t")
        val t = arr(1).toInt
        t == timestamp
      }.toJavaRDD(), StorageLevel.MEMORY_ONLY, sepsg, tepsg)
    }
    points.analyze()
    points.CRSTransform(sepsg, tepsg)
    val nPoints = points.rawSpatialRDD.count()
    logEnd(stage, timer, nPoints)

    // Creating unifrom distribution
    val n = 100000
    val seed = 42

    val bounds = points.boundaryEnvelope
    val minX = bounds.getMinX
    val minY = bounds.getMinY
    val maxX = bounds.getMaxX
    val maxY = bounds.getMaxY

    logger.info(s"Bounds are: ($minX, $minY) and  ($maxX, $maxY)")

    val width  = maxX - minX
    val height = maxY - minY

    logger.info(s"Dimensions are: $width x $height")

    val ids = 0 until n
    val df = spark.createDataset(ids).toDF("id")
    val dataset = df.select($"id", rand() as "x", rand() as "y")
      .map{ p =>
        val id = p.getInt(0)
        val x = p.getDouble(1)
        val y = p.getDouble(2)
        val t = 0

        ST_Point(id, roundAt(3)(minX + (x * width)), roundAt(3)(minY + (y * height)), t)
      }.cache()
    val nDataset = dataset.count()
    logger.info(s"Original count: $nDataset")

    val dataset2 = dataset.groupBy($"x", $"y", $"t").agg(min($"id").alias("id"))
      .select($"id", $"x", $"y", $"t").as[ST_Point]
      .cache
    val nDataset2 = dataset2.count()
    logger.info(s"New count: $nDataset2")

    val data = dataset2.map(p => s"${p.id}\t${p.x}\t${p.y}\t${p.t}\n").collect().mkString("")
    val f = new java.io.PrintWriter("/tmp/uniform.tsv")
    f.write(data)
    f.close()

    // Closing session...
    timer = System.currentTimeMillis()
    stage = "Session closed"
    logStart(stage)
    if(info){
      InfoTracker.master = host
      InfoTracker.port = portUI
      InfoTracker.applicationID = appID
      InfoTracker.executors = executors
      InfoTracker.cores = cores
      val app_count = appID.split("-").reverse.head
      val f = new java.io.PrintWriter(s"${params.output()}app-${app_count}_info.tsv")
      f.write(InfoTracker.getExectutorsInfo())
      f.write(InfoTracker.getStagesInfo())
      f.write(InfoTracker.getTasksInfoByDuration(25))
      f.close()
    }
    spark.close()
    logEnd(stage, timer, 0)
  }  
}
