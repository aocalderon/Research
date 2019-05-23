import com.vividsolutions.jts.geom.{Point, Geometry, GeometryFactory, Coordinate, Envelope, Polygon}
import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialPartitioning.FlatGridPartitioner
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import SPMF.{AlgoLCM2, Transactions}
import SPMF.ScalaLCM.{IterativeLCMmax, Transaction}
import java.io.PrintWriter

object GridExpander{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val precision: Double = 0.001
  private var tag: String = ""
  private var appID: String = "app-00000000000000-0000"
  private var startTime: Long = clocktime
  private var cores: Int = 0
  private var executors: Int = 0
  private val factor: Double = 1000.0

  def run(spark: SparkSession, points: PointRDD, params: FFConf, info: String = ""): RDD[String] = {
    import spark.implicits._
    var expansions = spark.sparkContext.emptyRDD[String]

    appID     = spark.sparkContext.applicationId
    startTime = spark.sparkContext.startTime
    cores     = params.cores()
    executors = params.executors()
    val debug: Boolean    = params.mfdebug()
    val epsilon: Double   = params.epsilon()
    val sespg: String     = params.sespg()
    val tespg: String     = params.tespg()
    val spatial: String   = params.spatial()
    val partitioner = spatial  match {
      case "QUADTREE"  => GridType.QUADTREE
      case "RTREE"     => GridType.RTREE
      case "EQUALGRID" => GridType.EQUALGRID
      case "KDBTREE"   => GridType.KDBTREE
      case "HILBERT"   => GridType.HILBERT
      case "VORONOI"   => GridType.VORONOI
      case "CUSTOM"    => GridType.CUSTOM
    }

    points.analyze()
    points.setNumX(params.mfcustomx().toInt)
    points.setNumY(params.mfcustomy().toInt)
    points.spatialPartitioning(partitioner, 200)
    val cell = points.getPartitioner.getGrids.asScala.head
    val width  = (cell.getWidth * factor).toInt
    val height = (cell.getHeight * factor).toInt
    val minX = (points.boundary().getMinX * factor).toInt
    val minY = (points.boundary().getMinY * factor).toInt
    val cellsInX = params.mfcustomx().toInt
    val e = (epsilon * factor) / 2.0
    logger.info(s"Origin: ($minX , $minY)")
    logger.info(s"Cell size: $width x $height")
    logger.info(s"# of cells in X: $cellsInX")

    val expansions1 = points.getRawSpatialRDD.rdd.map{ point =>
      val i = ((point.getX * factor).toInt - minX) / width
      val j = ((point.getY * factor).toInt - minY) / height
      val location = locatePointInCell(point, width, height, minX, minY, e)
      (point.toText, i.toInt, j.toInt, i.toInt + (j.toInt * cellsInX), location)
    }
    expansions1.toDF("Point", "i","j","id", "location").show(25, false)

    expansions = expansions1.map(e => s"${e._1}\t${e._4}\t${e._5}\n")
    expansions
  }

  def locatePointInCell(p: Point, w: Int, h: Int, mx: Int, my: Int, e: Double): String = {
    val x = ((p.getX * factor).toInt - mx) % w
    val y = ((p.getY * factor).toInt - my) % h

    var location = s"*"
    if(x <= w - e && x >= e     && y <= h - e && y >= e){
      location = "C"
    } else if(x <  e     && x >= 0     && y <= h - e && y >= e    ){
      location = "W"
    } else if(x <  w     && x >  w - e && y <= h - e && y >= e    ){
      location = "E"
    } else if(x <= w - e && x >= e     && y <  h     && y >  h - e){
      location = "N"
    } else if(x <= w - e && x >= e     && y <  e     && y >= 0    ){
      location = "S"
    }

    location
  }

  def isNotInExpansionArea(p: Point, w: Int, h: Int, mx: Int, my: Int, e: Double): Boolean = {
    val x = (p.getX * factor).toInt - mx
    val y = (p.getY * factor).toInt - my

    x <= w - e && x >= e && y <= h - e && y >= e
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    val duration = (clocktime - startTime) / 1000.0
    logger.info("GRIDEXPANDER|%-30s|%6.2f|%-50s|%6.2f|%6d|%s".format(s"$appID|$executors|$cores|", duration, msg, (clocktime - timer) / 1000.0, n, status))
  }

  import java.io._
  def saveWKT(wkt: RDD[String], filename: String): Unit ={
    val pw = new PrintWriter(new File(filename))
    pw.write(wkt.collect().mkString(""))
    pw.close
  }

  import org.apache.spark.Partitioner
  class ExpansionPartitioner(partitions: Int) extends Partitioner{
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      key.asInstanceOf[Int]
    }
  }

  /***
   * The main function...
   **/
  def main(args: Array[String]) = {
    val params      = new FFConf(args)
    val master      = params.master()
    val port        = params.port()
    val input       = params.input()
    val offset      = params.offset()
    val sepsg       = params.sespg()
    val tepsg       = params.tespg()
    val timestamp   = params.timestamp()
    val Dpartitions = (cores * executors) * params.dpartitions()
    cores       = params.cores()
    executors   = params.executors()

    // Starting session...
    var timer = clocktime
    var stage = "Session started"
    log(stage, timer, 0, "START")
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * cores * executors)
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.cores.max", cores * executors)
      .config("spark.executor.cores", cores)
      .master(s"spark://${master}:${port}")
      .appName("GridExpander")
      .getOrCreate()
    import spark.implicits._
    appID = spark.sparkContext.applicationId
    startTime = spark.sparkContext.startTime
    log(stage, timer, 0, "END")

    // Reading data...
    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    var points = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true, Dpartitions)
    if(timestamp >= 0){
      points = new PointRDD(points.rawSpatialRDD.rdd.filter{p =>
        val arr = p.getUserData.toString().split("\t")
        val t = arr(1).toInt
        t == timestamp
      }.toJavaRDD(), StorageLevel.MEMORY_ONLY, sepsg, tepsg)
    }
    points.CRSTransform(sepsg, tepsg)
    val nPoints = points.rawSpatialRDD.count()
    log(stage, timer, nPoints, "END")

    // Running maximal finder...
    timer = clocktime
    stage = "Grid expansions"
    log(stage, timer, 0, "START")
    val expansions = GridExpander.run(spark, points, params)
    val nExpansions = expansions.count()
    log(stage, timer, nExpansions, "END")

    val filename = s"${input.split("/").reverse.head.split("\\.").head}_expansions.wkt"
    GridExpander.saveWKT(expansions, filename)

    // Closing session...
    timer = System.currentTimeMillis()
    stage = "Session closed"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }  
}
