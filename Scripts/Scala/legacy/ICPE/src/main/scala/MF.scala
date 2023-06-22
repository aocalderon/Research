import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PolygonRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialPartitioning.{KDBTree, KDBTreePartitioner}
import org.datasyslab.geospark.spatialPartitioning.quadtree.{QuadTreePartitioner, StandardQuadTree, QuadRectangle}
import org.datasyslab.geospark.utils.RDDSampleUtils
import com.vividsolutions.jts.operation.buffer.BufferParameters
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Envelope, Coordinate, Polygon, LinearRing, Point}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import SPMF.{AlgoLCM2, Transactions, Transaction}
import org.rogach.scallop._

object MF{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val reader = new com.vividsolutions.jts.io.WKTReader(geofactory)
  private val precision: Double = 0.001
  private var tag: String = ""
  private var appID: String = "app-00000000000000-0000"
  private var startTime: Long = clocktime
  private var cores: Int = 0
  private var executors: Int = 0

  def run(spark: SparkSession, points: SpatialRDD[Point], params: FFConf, timestamp: Int = -1, info: String = ""): (RDD[Point], Long) = {
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
    if(params.tag() == ""){ tag = s"$info"} else { tag = s"${params.tag()}|${info}" }
    var Dpartitions: Int  = params.dpartitions()

    // Indexing points...
    val localStart = clocktime
    var timer = System.currentTimeMillis()
    var stage = "A.Points indexed"
    logStart(stage)

    points.spatialPartitioning(GridType.QUADTREE, params.ffpartitions())
    val nMF1Grids = points.getPartitioner.getGrids.size()

    val pointsBuffer = new CircleRDD(points, epsilon + precision)
    pointsBuffer.analyze()
    pointsBuffer.spatialPartitioning(points.getPartitioner)
    points.buildIndex(IndexType.QUADTREE, true) // QUADTREE works better as an indexer than RTREE..
    points.indexedRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    points.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    logEnd(stage, timer, nMF1Grids)

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
    val centersRDD = new PointRDD(centers.toJavaRDD(), StorageLevel.MEMORY_ONLY)
    val centersBuffer = new CircleRDD(centersRDD, r + precision)
    centersBuffer.spatialPartitioning(points.getPartitioner)
    centersBuffer.buildIndex(IndexType.QUADTREE, true) // QUADTREE works better as an indexer than RTREE..
    centersBuffer.indexedRDD.cache()
    centersBuffer.spatialPartitionedRDD.cache()
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
      }.distinct().cache()
    val nDisks = disks.count()
    logEnd(stage, timer, nDisks)

    points.spatialPartitionedRDD.unpersist(false)
    pairs.unpersist(false)
    centers.unpersist(false)
    centersBuffer.spatialPartitionedRDD.unpersist(false)

    // Partition disks...
    timer = System.currentTimeMillis()
    stage = "E.Disks partitioned"
    logStart(stage)
    val diskCenters = new PointRDD(disks.toJavaRDD(), StorageLevel.MEMORY_ONLY)
    val diskCircles = new CircleRDD(diskCenters, r + precision)
    diskCircles.analyze()
    val (quadtree, nMF2Grids) = if(params.mfgrid()){
      diskCircles.spatialPartitioning(GridType.QUADTREE, params.mfpartitions())
      val nGrids = diskCircles.getPartitioner.getGrids.size()
      if(debug){ logger.info(s"Disks' number of cells: ${nGrids}") }
      (diskCircles.partitionTree, nGrids) 
    } else {
      val fullBoundary = diskCircles.boundary()
      fullBoundary.expandBy(epsilon + precision)
      val fraction = params.fraction()
      
      val samples = diskCircles.rawSpatialRDD.rdd
        .sample(false, fraction, 42)
        .map(_.getEnvelopeInternal)
      val boundary = new QuadRectangle(fullBoundary)
      val maxLevel = params.levels()
      val maxItemsPerNode = params.entries()
      val quadtree = new StandardQuadTree[Geometry](boundary, 0, maxItemsPerNode, maxLevel)
      if(debug){ logger.info(s"Disks' size of sample: ${samples.count()}") }
      for(sample <- samples.collect()){
        quadtree.insert(new QuadRectangle(sample), null)
      }
      quadtree.assignPartitionIds()
      val QTPartitioner = new QuadTreePartitioner(quadtree)
      val nGrids = QTPartitioner.getGrids.size
      if(debug) { logger.info(s"Disks' number of cells: ${nGrids}") }

      diskCircles.spatialPartitioning(QTPartitioner)

      (quadtree, nGrids)
    }
    diskCircles.spatialPartitionedRDD.cache()
    val nDisksRDD = diskCircles.spatialPartitionedRDD.count()
    logEnd(stage, timer, nDisksRDD)

    // Finding maximal disks...
    timer = System.currentTimeMillis()
    stage = "F.Maximal disks found"
    logStart(stage)
    val grids = quadtree.getAllZones.asScala.filter(_.partitionId != null)
      .map(r => r.partitionId -> r.getEnvelope).toMap
    val maximals = diskCircles.spatialPartitionedRDD.rdd
      .mapPartitionsWithIndex{ (i, disks) => 
        val transactions = disks.map{ d =>
          val x = d.getCenterPoint.x
          val y = d.getCenterPoint.y
          val pids = d.getUserData.toString()
          new Transaction(x, y, pids)
        }.toList.asJava
        val LCM = new AlgoLCM2()
        val data = new Transactions(transactions, 0)
        LCM.run(data)
        LCM.getPointsAndPids.asScala
          .map{ p =>
            val pids = p.getItems.mkString(" ")
            val x    = p.getX
            val y    = p.getY
            val t    = timestamp
            val grid = grids(i)
            val point = geofactory.createPoint(new Coordinate(x, y))
            val flag = isNotInExpansionArea(point, grid, 0.0)
            point.setUserData(s"$pids;$timestamp;$timestamp")
            (point, flag)
          }.filter(_._2).map(_._1).toIterator
      }.persist(StorageLevel.MEMORY_ONLY_SER)
    val nMaximals = maximals.count()
    logEnd(stage, timer, nMaximals)

    val localEnd = clocktime
    val executionTime = (localEnd - localStart) / 1000.0
    logger.info(s"MAXIMALS|$appID|$cores|$executors|$epsilon|$mu|$nMF1Grids|$nMF2Grids|$executionTime|$nMaximals")
    diskCircles.spatialPartitionedRDD.unpersist(false)

    (maximals, nMaximals)
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

  def clocktime = System.currentTimeMillis()

  def logEnd(msg: String, timer: Long, n: Long): Unit ={
    val duration = (clocktime - startTime) / 1000.0
    logger.info("MF|%-30s|%6.2f|%-30s|%6.2f|%6d|%s".format(s"$appID|$executors|$cores|  END", duration, msg, (System.currentTimeMillis()-timer)/1000.0, n, tag))
  }

  def logStart(msg: String): Unit ={
    val duration = (clocktime - startTime) / 1000.0
    logger.info("MF|%-30s|%6.2f|%-30s|%6.2f|%6d|%s".format(s"$appID|$executors|$cores|START", duration, msg, 0.0, 0, tag))
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

  def roundAt(p: Int)(n: Double): Double = { val s = math pow (10, p); (math round n * s) / s }

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def readGridCell(wkt: String): Envelope = {
    reader.read(wkt).getEnvelopeInternal
  }

  /***
   * The main function...
   **/
  def main(args: Array[String]) = {
    val params: FFConf = new FFConf(args)
    val input       = params.input()
    val output      = params.output()
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
    val debug       = params.mfdebug()
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
      //.config("spark.cores.max", cores * executors)
      //.config("spark.executor.cores", cores)
      //.master(master)
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
    val nPoints = points.rawSpatialRDD.count()
    logEnd(stage, timer, nPoints)
    
    // Running maximal finder...
    timer = System.currentTimeMillis()
    stage = "Maximal finder run"
    logStart(stage)
    val maximals = MF.run(spark, points, params)
    logEnd(stage, timer, maximals._2)

    if(debug){
      val filename = new java.io.File(input).getName()
      val regex = "(\\d+)".r
      val t = regex.findFirstIn(filename).get
      val data = maximals._1.map{ m =>
        val pids = m.getUserData.toString().split(";")(0)
        val x = roundAt(3)(m.getX)
        val y = roundAt(3)(m.getY)
        s"$t\t$x\t$y\t$pids\n"
      }.collect().sorted
      val f = new java.io.PrintWriter(s"${output}Disks_${t}.tsv")
      f.write(data.mkString(""))
      f.close()
      logger.info(s"Disks from $filename saved [${data.size} records]")
    }

    // Closing session...
    timer = System.currentTimeMillis()
    stage = "Session closed"
    logStart(stage)
    spark.close()
    logEnd(stage, timer, 0)
  }  
}
