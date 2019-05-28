import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialPartitioning.FlatGridPartitioner
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Envelope, Coordinate, Polygon, LinearRing, Point}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
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
  var expansions: List[(Envelope, Int)] = List.empty[(Envelope, Int)]

  def run(spark: SparkSession, disks: PointRDD, grids: List[Envelope], cellsInX: Int, params: FFConf): RDD[(Int, Point)] = {
    import spark.implicits._
    startTime = spark.sparkContext.startTime
    appID = spark.sparkContext.applicationId
    cores = params.cores()
    executors = params.executors()
    val epsilon: Double   = params.epsilon()
    val debug: Boolean    = params.mfdebug()

    if(debug){
      saveGrids(grids.toList, "/tmp/grids.wkt")
    }
    val cell = grids.head
    val width  = (cell.getWidth * factor).toInt
    val height = (cell.getHeight * factor).toInt
    val minX = (disks.boundary().getMinX * factor).toInt
    val minY = (disks.boundary().getMinY * factor).toInt
    setExpansions(grids, width, height, minX, minY, cellsInX, epsilon)
    val e = (epsilon * factor) / 2.0
    logger.info(s"Cell size: $width x $height")
    if(debug){
      logger.info(s"Origin: ($minX , $minY)")
      logger.info(s"Cell size: $width x $height")
      logger.info(s"# of cells in X: $cellsInX")
    }
    val expansions = disks.spatialPartitionedRDD.rdd.flatMap{ disk =>
      val i = (((disk.getX * factor).toInt - minX) / width).toInt
      val j = (((disk.getY * factor).toInt - minY) / height).toInt

      getDiskExpansions(disk, width, height, minX, minY, e, i, j, cellsInX)
    }.filter(_._1 >= 0)
    if(debug){
      expansions.map(e => (e._1, e._2.toText())).toDF("Partition","Point").show(false)
      saveExpansions(expansions, "/tmp/expansions.wkt")
    }

    expansions
  }

  def getDiskExpansions(p: Point, w: Int, h: Int, mx: Int, my: Int, e: Double, i: Int, j: Int, c: Int): List[(Int, Point)] = {
    val x = ((p.getX * factor).toInt - mx) % w
    val y = ((p.getY * factor).toInt - my) % h

    var location = s"*"
    var partitions = new ListBuffer[Int]()
    if(x <= w - e && x >= e     && y <= h - e && y >= e){
      location = "C"
      partitions += i + c * j
    } else if(x <  e     && x >= 0     && y <= h - e && y >= e    ){
      location = "W"
      partitions += i + c * j
      partitions += (i - 1) + c * j
    } else if(x <  w     && x >  w - e && y <= h - e && y >= e    ){
      location = "E"
      partitions += i + c * j
      partitions += (i + 1) + c * j
    } else if(x <= w - e && x >= e     && y <  h     && y >  h - e){
      location = "N"
      partitions += i + c * j
      partitions += i + c * (j + 1)
    } else if(x <= w - e && x >= e     && y <  e     && y >= 0    ){
      location = "S"
      partitions += i + c * j
      partitions += i + c * (j - 1)
    } else if(x <  e     && x >= 0     && y <  h     && y >  h - e){
      location = "NW"
      partitions += i + c * j
      partitions += i + c * (j + 1)
      partitions += (i - 1) + c * j
      partitions += (i - 1) + c * (j + 1)
    } else if(x <  w     && x >  w - e && y <  h     && y >  h - e){
      location = "NE"
      partitions += i + c * j
      partitions += i + c * (j + 1)
      partitions += (i + 1) + c * j
      partitions += (i + 1) + c * (j + 1)
    } else if(x <  e     && x >= 0     && y <  e     && y >= 0    ){
      location = "SW"
      partitions += i + c * j
      partitions += i + c * (j - 1)
      partitions += (i - 1) + c * j
      partitions += (i - 1) + c * (j - 1)
    } else if(x <  w     && x >= w - e && y <  e     && y >= 0    ){
      location = "SE"
      partitions += i + c * j
      partitions += i + c * (j - 1)
      partitions += (i + 1) + c * j
      partitions += (i + 1) + c * (j - 1)
    }

    partitions.toList.map(partition => (partition, p))
  }

  def setExpansions(g: List[Envelope], w:Int, h: Int, mx: Int, my: Int, c: Int, e: Double): Unit = {
    expansions = g.map{ g =>
      val p = g.centre()
      val i = (((p.x * factor).toInt - mx) / w).toInt
      val j = (((p.y * factor).toInt - my) / h).toInt
      g.expandBy(e)
      (g, i + j * c)
    }
  }

  def envelope2Polygon(e: Envelope): Polygon = {
    val minX = e.getMinX()
    val minY = e.getMinY()
    val maxX = e.getMaxX()
    val maxY = e.getMaxY()
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(minX, maxY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(maxX, minY)
    val coordArraySeq = new CoordinateArraySequence( Array(p1,p2,p3,p4,p1), 2)
    val ring = new LinearRing(coordArraySeq, geofactory)
    new Polygon(ring, null, geofactory)
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    val duration = (clocktime - startTime) / 1000.0
    logger.info("GE|%-30s|%6.2f|%-50s|%6.2f|%6d|".format(s"$appID|$executors|$cores|$status", duration, msg, (clocktime - timer) / 1000.0, n, status))
  }

  import java.io._
  def saveWKT(wkt: RDD[String], filename: String): Unit ={
    val pw = new PrintWriter(new File(filename))
    pw.write(wkt.collect().mkString(""))
    pw.close
  }

  def saveExpansions(expansions: RDD[(Int, Point)], filename: String): Unit ={
    val pw = new PrintWriter(new File(filename))
    pw.write(expansions.map(e => s"${e._2.toText()}\t${e._2.getUserData.toString()}\t${e._1}\n").collect().mkString(""))
    pw.close
  }

  def saveGrids(grids: List[Envelope], filename: String): Unit ={
    val wkt = grids.map(g => s"${envelope2Polygon(g).toText()}\n")
    val pw = new PrintWriter(new File(filename))
    pw.write(wkt.mkString(""))
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
    val X = params.mfcustomx().toInt
    val Y = params.mfcustomy().toInt
    points.setNumX(X)
    points.setNumY(Y)
    points.spatialPartitioning(GridType.CUSTOM, X*Y)
    points.spatialPartitionedRDD.cache()
    val grids = points.getPartitioner.getGrids.asScala.toList
    val expansions = GridExpander.run(spark, points, grids, X, params)
    val nExpansions = expansions.count()
    log(stage, timer, nExpansions, "END")

    GridExpander.saveExpansions(expansions, "/tmp/expansions.tsv")

    // Closing session...
    timer = System.currentTimeMillis()
    stage = "Session closed"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }  
}
