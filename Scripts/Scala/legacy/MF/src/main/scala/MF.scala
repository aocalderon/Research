import com.vividsolutions.jts.geom.{Point, Geometry, GeometryFactory, Coordinate, Envelope, Polygon}
import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD}
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

object MF{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val precision: Double = 0.001
  private var tag: String = ""

  def run(spark: SparkSession, points: PointRDD, params: FFConf, info: String = ""): Unit = {
    import spark.implicits._

    val debug: Boolean    = params.mfdebug()
    val epsilon: Double   = params.epsilon()
    val mu: Int           = params.mu()
    val sespg: String     = params.sespg()
    val tespg: String     = params.tespg()
    val cores: Int        = params.cores()
    val executors: Int    = params.executors()
    val Dpartitions: Int  = (cores * executors) * params.dpartitions()
    val MFpartitions: Int = params.mfpartitions()
    val spatial: String   = params.spatial()
    val partitioner = spatial  match {
      case "QUADTREE"  => GridType.QUADTREE
      case "RTREE"     => GridType.RTREE
      case "EQUALGRID" => GridType.EQUALGRID
      case "KDBTREE"   => GridType.KDBTREE
      case "HILBERT"   => GridType.HILBERT
      case "VORONOI"   => GridType.VORONOI
    }
    if(params.tag() == ""){ tag = s"$info"} else { tag = s"${params.tag()}|${info}" }

    // Indexing points...
    var timer = System.currentTimeMillis()
    val pointsBuffer = new CircleRDD(points, epsilon + precision)
    points.analyze()
    pointsBuffer.analyze()
    pointsBuffer.spatialPartitioning(GridType.QUADTREE, Dpartitions)
    points.spatialPartitioning(pointsBuffer.getPartitioner)
    points.buildIndex(IndexType.QUADTREE, true)
    points.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    pointsBuffer.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    log("A.Points indexed", timer, points.rawSpatialRDD.count())

    // Finding pairs...
    timer = System.currentTimeMillis()
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
    log("B.Pairs found", timer, nPairs)

    // Finding centers...
    timer = System.currentTimeMillis()
    val r2: Double = math.pow(epsilon / 2.0, 2)
    val centersPairs = pairs.map{ p =>
        val p1 = p._1._2
        val p2 = p._2._2
        calculateCenterCoordinates(p1, p2, r2)
    }
    val centers = centersPairs.map(_._1).union(centersPairs.map(_._2))
    val nCenters = centers.count()
    log("C.Centers found", timer, nCenters)

    // Finding disks...
    timer = System.currentTimeMillis()
    val r = epsilon / 2.0
    val centersRDD = new PointRDD(centers.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
    val centersBuffer = new CircleRDD(centersRDD, r + precision)
    centersBuffer.analyze()
    centersBuffer.spatialPartitioning(points.getPartitioner)
    centersBuffer.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

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
    log("D.Disks found", timer, nDisks)

    // Indexing disks...
    timer = System.currentTimeMillis()
    val disksRDD = new PointRDD(disks.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
    disksRDD.analyze()
    val nDisksRDD = disksRDD.rawSpatialRDD.count()
    disksRDD.spatialPartitioning(partitioner, MFpartitions)
    disksRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    log("E.Disks indexed", timer, nDisksRDD)

    if(debug){
      logger.info(s"Saving candidate set $tag...")
      val data = disksRDD.rawSpatialRDD.rdd.map{ p =>
        val pids = p.getUserData.toString
        val x = p.getX
        val y = p.getY
        s"${x}\t${y}\t${pids}\n"
      }.collect().mkString("")
      
      val pw = new java.io.PrintWriter(s"/tmp/PartMF_E${epsilon}_N${executors}.tsv")
      pw.write(data)
      pw.close
    }

    // Getting overlapping disks...
    timer = clocktime
    val disksRDDBuffer = new CircleRDD(disksRDD, epsilon + precision)
    disksRDD.analyze()
    disksRDDBuffer.analyze()
    disksRDDBuffer.spatialPartitioning(GridType.QUADTREE, Dpartitions)
    disksRDD.spatialPartitioning(disksRDDBuffer.getPartitioner)
    disksRDD.buildIndex(IndexType.QUADTREE, true)
    disksRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    disksRDDBuffer.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    val overlaps = JoinQuery.DistanceJoinQueryFlat(disksRDD, disksRDDBuffer, usingIndex, considerBoundary)
      .rdd.map{ pair =>
        val id1 = pair._1.getUserData().toString()//.split("\t").head.trim().toInt
        val p1  = pair._1.getCentroid.toString()
        val id2 = pair._2.getUserData().toString()//.split("\t").head.trim().toInt
        val p2  = pair._2.getCentroid.toString()
        ( p1 , id2 )
      }.toDF("id", "pids").groupBy($"id").agg(count($"pids").as("count"))
    val nOverlaps = overlaps.count()
    log("F.Overlaping disks gotten", timer, nOverlaps)

    import org.apache.spark.sql.functions._
    overlaps.orderBy(desc("count")).show(10, false)
  }

  def clocktime  = System.currentTimeMillis()

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

  def log(msg: String, timer: Long, n: Long = 0): Unit ={
    if(n == 0)
      logger.info("%-50s|%6.2f".format(msg,(System.currentTimeMillis()-timer)/1000.0))
    else
      logger.info("%-50s|%6.2f|%6d|%s".format(msg,(System.currentTimeMillis()-timer)/1000.0,n,tag))
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
    val input       = params.input()
    val offset      = params.offset()
    val sepsg       = params.sespg()
    val tepsg       = params.tespg()
    val timestamp   = params.timestamp()
    val cores       = params.cores()
    val executors   = params.executors()
    val Dpartitions = (cores * executors) * params.dpartitions()

    // Starting session...
    var timer = System.currentTimeMillis()
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.cores.max", cores * executors)
      .config("spark.executor.cores", cores)
      .master(master).appName("MaximalFinder")
      .getOrCreate()
    import spark.implicits._
    val appID = spark.sparkContext.applicationId
    logger.info(s"Session started [${(System.currentTimeMillis - timer) / 1000.0}]")

    // Reading data...
    timer = System.currentTimeMillis()
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
    logger.info(s"Data read [${(System.currentTimeMillis - timer) / 1000.0}] [$nPoints]")

    // Running maximal finder...
    timer = System.currentTimeMillis()
    MF.run(spark, points, params)
    logger.info(s"Maximal finder run [${(System.currentTimeMillis() - timer) / 1000.0}]")

    // Closing session...
    timer = System.currentTimeMillis()
    spark.stop()
    logger.info(s"Session closed [${(System.currentTimeMillis - timer) / 1000.0}]")
  }  
}
