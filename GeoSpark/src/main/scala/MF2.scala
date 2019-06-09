import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialPartitioning.FlatGridPartitioner
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PolygonRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.operation.buffer.BufferParameters
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Envelope, Coordinate, Polygon, LinearRing, Point}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import SPMF.{AlgoLCM2, Transactions, Transaction}

object MF2{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val precision: Double = 0.001
  private var tag: String = ""
  private var appID: String = "app-00000000000000-0000"
  private var startTime: Long = clocktime
  private var cores: Int = 0
  private var executors: Int = 0

  def run(spark: SparkSession, points: PointRDD, params: FFConf, info: String = ""): RDD[String] = {
    import spark.implicits._

    appID     = spark.sparkContext.applicationId
    startTime = spark.sparkContext.startTime
    cores     = params.cores()
    executors = params.executors()
    val debug: Boolean    = params.mfdebug()
    val epsilon: Double   = params.epsilon()
    val mu: Int           = params.mu()
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
    if(params.tag() == ""){ tag = s"$info"} else { tag = s"${params.tag()}|${info}" }
    var Dpartitions: Int  = (cores * executors) * params.dpartitions()
    var MFpartitions: Int = params.mfpartitions()

    // Indexing points...
    var timer = System.currentTimeMillis()
    var stage = "A.Points indexed"
    logStart(stage)

    val pointsBuffer = new CircleRDD(points, epsilon + precision)
    points.analyze()
    pointsBuffer.analyze()
    points.spatialPartitioning(GridType.KDBTREE, Dpartitions) // KDBTREE works better that CUSTOM and QUADTREE at this point...
    pointsBuffer.spatialPartitioning(points.getPartitioner)
    points.buildIndex(IndexType.QUADTREE, true) 
    points.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    pointsBuffer.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    logEnd(stage, timer, points.rawSpatialRDD.count())

    // Finding pairs...
    timer = System.currentTimeMillis()
    stage = "B.Pairs found"
    logStart(stage)
    val considerBoundary = true
    val usingIndex = true
    val pairs = JoinQuery.DistanceJoinQueryFlat(points, pointsBuffer, usingIndex, considerBoundary)
      .rdd.map{ pair =>
        val id1 = pair._1.getUserData().toString().split("\t").head.trim().toInt
        val p1  = pair._1.getCentroid
        val id2 = pair._2.getUserData().toString().split("\t").head.trim().toInt
        val p2  = pair._2
        ( (id1, p1) , (id2, p2) )
      }.filter(p => p._1._1 < p._2._1)
    val nPairs = pairs.count()
    logEnd(stage, timer, nPairs)

    // Finding centers...
    timer = System.currentTimeMillis()
    stage = "C.Centers found"
    logStart(stage)
    val r2: Double = math.pow(epsilon / 2.0, 2)
    val centersPairs = pairs.map{ p =>
        val p1 = p._1._2
        val p2 = p._2._2
        calculateCenterCoordinates(p1, p2, r2)
    }
    val centers = centersPairs.map(_._1).union(centersPairs.map(_._2))
    val nCenters = centers.count()
    logEnd(stage, timer, nCenters)

    // Finding disks...
    timer = System.currentTimeMillis()
    stage = "D.Disks found"
    logStart(stage)
    val r = epsilon / 2.0
    val centersRDD = new PointRDD(centers.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
    val centersBuffer = new CircleRDD(centersRDD, r + precision)
    centersBuffer.spatialPartitioning(points.getPartitioner)

    timer = System.currentTimeMillis()
    val disks = JoinQuery.DistanceJoinQueryFlat(points, centersBuffer, usingIndex, considerBoundary)
      .rdd.map{ d =>
        val c = d._1.getEnvelope.getCentroid
        val pid = d._2.getUserData().toString().split("\t").head.trim()
        d._2.setUserData(s"$pid")
        (c, Array(d._2))
      }.reduceByKey( (pids1,pids2) => pids1 ++ pids2)
      .map(_._2)
      .filter(d => d.length >= mu)
      .map{ d =>
        val pids = d.map(_.getUserData.toString().toInt).sorted.mkString(" ")
        val centroid = geofactory.createMultiPoint(d).getEnvelope().getCentroid
        centroid.setUserData(pids)
        centroid
      }.distinct()
    val nDisks = disks.count()
    logEnd(stage, timer, nDisks)

    // Partition disks...
    timer = System.currentTimeMillis()
    stage = "E.Disks partitioned"
    logStart(stage)
    val diskCenters = new PointRDD(disks.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
    val diskCircles = new CircleRDD(diskCenters, r + precision)
    val numX = params.mfcustomx().toInt
    val numY = params.mfcustomy().toInt
    diskCenters.setNumX(numX)
    diskCenters.setNumY(numY)
    diskCenters.analyze()
    if(spatial == "CUSTOM"){ MFpartitions = numX * numY }
    diskCenters.spatialPartitioning(partitioner, MFpartitions)
    diskCenters.spatialPartitionedRDD.cache()
    diskCircles.spatialPartitioning(diskCenters.getPartitioner)
    diskCircles.spatialPartitionedRDD.cache()
    val nDisksRDD = diskCenters.spatialPartitionedRDD.count()
    logEnd(stage, timer, nDisksRDD)

    // Finding maximal disks...
    timer = System.currentTimeMillis()
    stage = "F.Maximal disks found"
    logStart(stage)
    val grids = diskCenters.getPartitioner.getGrids.asScala.toList.zipWithIndex.map(g => g._2 -> g._1).toMap
    val g = new java.io.PrintWriter("/tmp/grids.wkt")
    g.write(grids.map(e => (e._1, envelope2Polygon(e._2).toText())).map(x => s"${x._1}\t${x._2}\n").mkString(""))
    g.close()
    val maximals = diskCircles.spatialPartitionedRDD.rdd
      .mapPartitionsWithIndex{ (i, disks) =>
        var result = List.empty[String]
        if(i == MFpartitions && spatial == "CUSTOM"){
        } else {
          val transactions = disks.map{ d =>
            val x = d.getCenterPoint.x
            val y = d.getCenterPoint.y
            val pids = d.getUserData.toString()
            new Transaction(x, y, pids)
          }.toList.asJava
          val LCM = new AlgoLCM2()
          val data = new Transactions(transactions, 0)
          LCM.run(data)
          result = LCM.getPointsAndPids.asScala
            .map{ p =>
              val pids = p.getItems.mkString(" ")
              val grid = grids(i)
              val point = geofactory.createPoint(new Coordinate(p.getX, p.getY))
              
              val flag = isNotInExpansionArea(point, grid, 0.0)
              ((p.getX, p.getY, pids),  flag)
            }
            .filter(_._2).map(_._1)
            .map(p => s"${p._1}\t${p._2}\t${p._3}\n").toList
        }
        result.toIterator
      }.persist(StorageLevel.MEMORY_ONLY)
    val nMaximals = maximals.count()
    logEnd(stage, timer, nMaximals)

    val endTime = System.currentTimeMillis()
    val totalTime = (endTime - startTime) / 1000.0

    //val f = new java.io.PrintWriter("/tmp/maximals.wkt")
    //f.write(maximals.collect().mkString(""))
    //f.close()

    maximals
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

  def calculateCenterCoordinates(p1: Point, p2: Point, r2: Double): (Point, Point) = {
    var h = geofactory.createPoint(new Coordinate(-1.0,-1.0))
    var k = geofactory.createPoint(new Coordinate(-1.0,-1.0))
    val X: Double = p1.getX - p2.getX
    val Y: Double = p1.getY - p2.getY
    val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
    if (D2 != 0.0){
      val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
      val h1: Double = ((X + Y * root) / 2) + p2.getX
      val k1: Double = ((Y - X * root) / 2) + p2.getY
      val h2: Double = ((X - Y * root) / 2) + p2.getX
      val k2: Double = ((Y + X * root) / 2) + p2.getY
      h = geofactory.createPoint(new Coordinate(h1,k1))
      k = geofactory.createPoint(new Coordinate(h2,k2))
    }
    (h, k)
  }

  def isNotInExpansionArea(p: Point, e: Envelope, epsilon: Double): Boolean = {
    val error = 0.00000000001
    val x = p.getX
    val y = p.getY
    val min_x = e.getMinX - error
    val min_y = e.getMinY - error
    val max_x = e.getMaxX
    val max_y = e.getMaxY

    x <= (max_x - epsilon) &&
      x >= (min_x + epsilon) &&
      y <= (max_y - epsilon) &&
      y >= (min_y + epsilon)
  }

  def castEnvelopeInt(x: Any): Tuple2[Envelope, Int] = {
    x match {
      case (e: Envelope, i: Int) => Tuple2(e, i)
    }
  }

  def castStringBoolean(x: Any): Tuple2[String, Boolean] = {
    x match {
      case (s: String, b: Boolean) => Tuple2(s, b)
    }
  }

  def clocktime = System.currentTimeMillis()

  def logEnd(msg: String, timer: Long, n: Long): Unit ={
    val duration = (clocktime - startTime) / 1000.0
    logger.info("MF|%-30s|%6.2f|%-50s|%6.2f|%6d|%s".format(s"$appID|$executors|$cores|  END", duration, msg, (System.currentTimeMillis()-timer)/1000.0, n, tag))
  }

  def logStart(msg: String): Unit ={
    val duration = (clocktime - startTime) / 1000.0
    logger.info("MF|%-30s|%6.2f|%-50s|%6.2f|%6d|%s".format(s"$appID|$executors|$cores|START", duration, msg, 0.0, 0, tag))
  }

  import java.io._
  def saveLines(data: RDD[String], filename: String): Unit ={
    val pw = new PrintWriter(new File(filename))
    pw.write(data.collect().mkString(""))
    pw.close
  }
  def saveList(data: List[String], filename: String): Unit ={
    val pw = new PrintWriter(new File(filename))
    pw.write(data.mkString(""))
    pw.close
  }
  def saveText(data: String, filename: String): Unit ={
    val pw = new PrintWriter(new File(filename))
    pw.write(data)
    pw.close
  }

  import org.apache.spark.Partitioner
  class ExpansionPartitioner(partitions: Int) extends Partitioner{
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      key.asInstanceOf[Int]
    }
  }

  import Numeric.Implicits._
  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)
    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

  /***
   * The main function...
   **/
  def main(args: Array[String]) = {
    val params      = new FFConf(args)
    val master      = params.master()
    val port        = params.port()
    val portUI      = params.portui()
    val input       = params.input()
    val offset      = params.offset()
    val sepsg       = params.sespg()
    val tepsg       = params.tespg()
    val info       = params.info()
    val timestamp   = params.timestamp()
    val Dpartitions = (cores * executors) * params.dpartitions()
    cores       = params.cores()
    executors   = params.executors()

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
      .master(s"spark://${master}:${port}")
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
    points.CRSTransform(sepsg, tepsg)
    val nPoints = points.rawSpatialRDD.count()
    logEnd(stage, timer, nPoints)

    // Running maximal finder...
    timer = System.currentTimeMillis()
    stage = "Maximal finder run"
    logStart(stage)
    val maximals = MF2.run(spark, points, params)
    val nMaximals = maximals.count()
    logEnd(stage, timer, nMaximals)

    // Closing session...
    timer = System.currentTimeMillis()
    stage = "Session closed"
    logStart(stage)
    if(info){
      InfoTracker.master = master
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
