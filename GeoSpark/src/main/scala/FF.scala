import com.vividsolutions.jts.geom.{Point, Geometry, GeometryFactory, Coordinate, Envelope, Polygon}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap

object FF {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val precision: Double = 0.001
  private var tag: String = ""

  case class Flock(pids: List[Int], start: Int, end: Int, center: Point)

  /* SpatialJoin variant */
  def runSpatialJoin(spark: SparkSession, pointset: HashMap[Int, PointRDD], params: FFConf): Unit = {
    val sespg = params.sespg()
    val tespg = params.tespg()
    val distance = params.distance()
    val dpartitions = params.dpartitions()
    val mu = params.mu()
    val delta = params.delta()
    var timer = System.currentTimeMillis()
    import spark.implicits._

    // Maximal disks timestamp i...
    var firstRun: Boolean = true
    var F: PointRDD = new PointRDD(spark.sparkContext.emptyRDD[Point].toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
    var partitioner = F.getPartitioner  
    for(timestamp <- pointset.keys.toList.sorted){
      val T_i = pointset.get(timestamp).get
      logger.info(s"Starting maximal disks timestamp $timestamp ...")
      timer = System.currentTimeMillis()
      val C = new PointRDD(MF.run(spark, T_i, params, s"$timestamp").map(c => makePoint(c, timestamp)).toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
      val nDisks = C.rawSpatialRDD.count()
      log(s"Maximal disks timestamp $timestamp", timer, nDisks)
      
      if(firstRun){
        F = C
        F.analyze()
        F.spatialPartitioning(GridType.KDBTREE, dpartitions)
        F.buildIndex(IndexType.QUADTREE, true)
        partitioner = F.getPartitioner
        firstRun = false
      } else {
        C.analyze()
        val buffers = new CircleRDD(C, distance)
        buffers.spatialPartitioning(partitioner)
        val R = JoinQuery.DistanceJoinQuery(F, buffers, true, false)
        var flocks = R.rdd.flatMap{ case (g: Geometry, h: java.util.HashSet[Point]) =>
          val f0 = getFlocksFromGeom(g)
          h.asScala.map{ point =>
            val f1 = getFlocksFromGeom(point)
            (f0, f1)
          }
        }.map{ flocks =>
          val f = flocks._1.pids.intersect(flocks._2.pids)
          val s = flocks._2.start
          val e = flocks._1.end
          val c = flocks._1.center
          Flock(f, s, e, c)
        }.filter(_.pids.length >= mu).distinct().persist()

        // Reporting flocks...
        val f0 =  flocks.filter{f => f.end - f.start + 1 >= delta}.persist()
        val f1 =  flocks.filter{f => f.end - f.start + 1 <  delta}.persist()
        f0.sortBy(_.pids.mkString(" "))
          .map(f => s"${f.start}, ${f.end}, ${f.pids.mkString(" ")}")
          .toDS().show(100, truncate=false)
        flocks = f0.map(f => Flock(f.pids, f.start + 1, f.end, f.center)).union(f1).persist()

        F = new PointRDD(flocks.map{ f =>
            val info = s"${f.pids.mkString(" ")};${f.start};${f.end}"
            f.center.setUserData(info)
            f.center
          }.union(C.rawSpatialRDD.rdd).toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
        F.spatialPartitioning(partitioner)
      }
    }
  }

  def getFlocksFromGeom(g: Geometry): Flock = {
    val farr = g.getUserData.toString().split(";")
    val pids = farr(0).split(" ").map(_.toInt).toList
    val start = farr(1).toInt
    val end = farr(2).toInt
    val center = g.getCentroid

    Flock(pids, start, end, center)
  }

  def main(args: Array[String]): Unit = {
    val params = new FFConf(args)
    val master = params.master()
    val input  = params.input()
    val offset = params.offset()
    val ppartitions = params.ppartitions()
    tag = params.tag()

    // Starting session...
    var timer = System.currentTimeMillis()
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(master).appName("PFLock").getOrCreate()
    logger.info(s"Session started [${(System.currentTimeMillis - timer) / 1000.0}]")

    // Reading data...
    timer = System.currentTimeMillis()
    val points = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true, ppartitions)
      points.CRSTransform(params.sespg(), params.tespg())
    val nPoints = points.rawSpatialRDD.count()
    val timestamps = points.rawSpatialRDD.rdd.map(_.getUserData.toString().split("\t").reverse.head.toInt).distinct.collect()
    val pointset: HashMap[Int, PointRDD] = new HashMap()
    for(timestamp <- timestamps){
      pointset += (timestamp -> extractTimestamp(points, timestamp, params.sespg(), params.tespg()))
    }
    logger.info(s"Data read [${(System.currentTimeMillis - timer) / 1000.0}] [$nPoints]")

    // Running maximal finder...
    timer = System.currentTimeMillis()
    runSpatialJoin(spark: SparkSession, pointset: HashMap[Int, PointRDD], params: FFConf)
    logger.info(s"Maximal finder run [${(System.currentTimeMillis() - timer) / 1000.0}]")

    // Closing session...
    timer = System.currentTimeMillis()
    spark.stop()
    logger.info(s"Session closed [${(System.currentTimeMillis - timer) / 1000.0}]")    
  }

  def extractTimestamp(points: PointRDD, timestamp:  Int, sespg: String, tespg: String): PointRDD = {
    new PointRDD(points.rawSpatialRDD.rdd.filter{
      _.getUserData().toString().split("\t").reverse.head.toInt == timestamp
    }.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
  }

  def makePoint(pattern: String, timestamp: Int): Point = {
    val arr = pattern.split(";")
    val point = geofactory.createPoint(new Coordinate(arr(1).toDouble, arr(2).toDouble))
    point.setUserData(s"${arr(0)};${timestamp};${timestamp}")
    point
  }

  def log(msg: String, timer: Long, n: Long = 0): Unit ={
    if(n == 0)
      logger.info("%-50s|%6.2f".format(msg,(System.currentTimeMillis()-timer)/1000.0))
    else
      logger.info("%-50s|%6.2f|%6d|%s".format(msg,(System.currentTimeMillis()-timer)/1000.0,n,tag))
  }
}
