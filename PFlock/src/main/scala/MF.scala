import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PolygonRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialPartitioning.{KDBTree, KDBTreePartitioner}
import org.datasyslab.geospark.spatialPartitioning.quadtree.{QuadTreePartitioner, StandardQuadTree, QuadRectangle}
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

  def run(spark: SparkSession, points: SpatialRDD[Point], params: FFConf, timestamp: Int = -1): (RDD[Point], Long) = {
    implicit val session = spark
    import spark.implicits._
    
    val epsilon: Double   = params.epsilon()
    val mu: Int           = params.mu()
    val debug: Boolean    = params.mfdebug()

    // Indexing points...
    val localStart = clocktime
    val stage1 = "A.Points indexed"
    logStart(stage1, timestamp)
    val timer1 = clocktime

    points.spatialPartitioning(GridType.QUADTREE, params.mfpartitions())
    val nMF1Grids = points.getPartitioner.getGrids.size()

    val pointsBuffer = new CircleRDD(points, epsilon + precision)
    pointsBuffer.analyze()
    pointsBuffer.spatialPartitioning(points.getPartitioner)
    points.buildIndex(IndexType.QUADTREE, true) // QUADTREE works better as an indexer than RTREE..
    points.indexedRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    points.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    logEnd(stage1, timer1, nMF1Grids, timestamp)

    // Finding pairs...
    val stage2 = "B.Pairs found"
    logStart(stage2, timestamp)
    val timer2 = clocktime
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
    logEnd(stage2, timer2, nPairs, timestamp)

    // Finding centers...
    val stage3 = "C.Centers found"
    logStart(stage3, timestamp)
    val timer3 = clocktime
    val r2: Double = math.pow(epsilon / 2.0, 2)
    val centersPairs = pairs.map{ p =>
        val p1 = p._1._2
        val p2 = p._2._2
        calculateCenterCoordinates(p1, p2, r2)
    }
    val centers = centersPairs.map(_._1).union(centersPairs.map(_._2))
    val nCenters = centers.count()
    logEnd(stage3, timer3, nCenters, timestamp)

    // Finding disks...
    val stage4 = "D.Disks found"
    logStart(stage4, timestamp)
    val timer4 = clocktime
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
    logEnd(stage4, timer4, nDisks, timestamp)

    points.spatialPartitionedRDD.unpersist(false)
    pairs.unpersist(false)
    centers.unpersist(false)
    centersBuffer.spatialPartitionedRDD.unpersist(false)

    // Partition disks...
    val stage5 = "E.Disks partitioned"
    logStart(stage5, timestamp)
    val timer5 = clocktime
    val diskCenters = new PointRDD(disks.toJavaRDD(), StorageLevel.MEMORY_ONLY)
    val diskCircles = new CircleRDD(diskCenters, r + precision)
    diskCircles.analyze()
    val (quadtree, nMF2Grids) = if(params.mfgrid()){
      if(debug){ logger.info("Building finer partitions by default quadtree...") }
      diskCircles.spatialPartitioning(GridType.QUADTREE, params.dpartitions())
      val nGrids = diskCircles.getPartitioner.getGrids.size()
      (diskCircles.partitionTree, nGrids) 
    } else {
      if(debug){ logger.info("Building finer partitions by custom quadtree...") }
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
      if(debug){ logger.info(s"Sample size during quadtree creation: ${samples.count()}") }
      for(sample <- samples.collect()){
        quadtree.insert(new QuadRectangle(sample), null)
      }
      quadtree.assignPartitionIds()
      val QTPartitioner = new QuadTreePartitioner(quadtree)
      val nGrids = QTPartitioner.getGrids.size

      diskCircles.spatialPartitioning(QTPartitioner)

      (quadtree, nGrids)
    }
    diskCircles.spatialPartitionedRDD.cache()
    val nDisksRDD = diskCircles.spatialPartitionedRDD.count()
    logEnd(stage5, timer5, nDisksRDD, timestamp)

    // Finding maximal disks...
    val stage6 = "F.Maximal disks found"
    logStart(stage6, timestamp)
    val timer6 = clocktime
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
    logEnd(stage6, timer6, nMaximals, timestamp)

    val localEnd = clocktime
    val executionTime = f"${(localEnd - localStart) / 1e9}%.2f"
    val (appId, cores, executors) = if(getConf("spark.master").contains("local")){
      val appId = getConf("spark.app.id")
      val cores = getConf("spark.master").split("\\[")(1).replace("]", "").toInt
      val executors = 1
      (appId, cores, executors)
    } else {
      val appId = getConf("spark.app.id").takeRight(4)
      val cores = getConf("spark.executor.instances").toInt
      val executors = getConf("spark.executor.cores").toInt
      (appId, cores, executors)
    }
    logger.info(s"MAXIMALS|$appId|$cores|$executors|$epsilon|$mu|$nMF1Grids|$nMF2Grids|$executionTime|$nMaximals")
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

  def clocktime = System.nanoTime()

  def getConf(param: String)(implicit spark: SparkSession): String = {
    spark.sparkContext.getConf.get(param)
  }

  def logStart(msg: String, timestamp: Int = -1)(implicit spark: SparkSession): Unit ={
    val startTime = spark.sparkContext.startTime 
    val duration = (System.currentTimeMillis() - startTime) / 1e3
    val (appId, cores, executors) = if(getConf("spark.master").contains("local")){
      val appId = getConf("spark.app.id")
      val cores = getConf("spark.master").split("\\[")(1).replace("]", "").toInt
      val executors = 1

      (appId, cores, executors)

    } else {
      val appId = getConf("spark.app.id").takeRight(4)
      val cores = getConf("spark.executor.instances").toInt
      val executors = getConf("spark.executor.cores").toInt

      (appId, cores, executors)
    }
    logger.info{
      "MF|%-4s|%d|%d|START|%6.2f|%-30s|%6.2f|%6d|%s".format(
        appId, executors, cores, duration, msg, 0.0, 0, timestamp
      )
    }
  }

  def logEnd(msg: String, timer: Long, n: Long, timestamp: Int = -1)(implicit spark: SparkSession): Unit ={
    val t = (clocktime - timer) / 1e9

    val startTime = spark.sparkContext.startTime
    val duration = (System.currentTimeMillis() - startTime) / 1e3
    val (appId, cores, executors) = if(getConf("spark.master").contains("local")){
      val appId = getConf("spark.app.id")
      val cores = getConf("spark.master").split("\\[")(1).replace("]", "").toInt
      val executors = 1

      (appId, cores, executors)

    } else {
      val appId = getConf("spark.app.id").takeRight(4)
      val cores = getConf("spark.executor.instances").toInt
      val executors = getConf("spark.executor.cores").toInt

      (appId, cores, executors)
    }
    logger.info{
      "MF|%-4s|%d|%d|  END|%6.2f|%-30s|%6.2f|%6d|%s".format(
        appId, executors, cores, duration, msg, t, n, timestamp
      )
    }
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
    val input  = params.input()
    val offset = params.offset()
    val debug  = params.mfdebug()

    // Starting session...
    logger.info("Starting session...")
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .appName("MF")
      .getOrCreate()
    import spark.implicits._
    logger.info("Starting session... Done!")

    logger.info(s"INFO|${getConf("spark.app.id").takeRight(4)}|${System.getProperty("sun.java.command")}")

    // Reading data...
    logger.info("Reading data...")
    val points = new SpatialRDD[Point]()
    val pointsRDD = spark.read.option("delimiter", "\t").option("header", false).csv(input).rdd
      .map{ row =>
        val id = row.getString(0)
        val x  = row.getString(1).toDouble
        val y  = row.getString(2).toDouble
        val t  = row.getString(3)

        val point = geofactory.createPoint(new Coordinate(x, y))
        point.setUserData(s"$id\t$t")
        point
      }
      .cache
    val nPointsRDD = pointsRDD.count()
    points.setRawSpatialRDD(pointsRDD)
    points.analyze()
    logger.info("Reading data... Done!")
    
    // Running maximal finder...
    val maximals = MF.run(spark, points, params, 0)

    // Closing session...
    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }  
}
