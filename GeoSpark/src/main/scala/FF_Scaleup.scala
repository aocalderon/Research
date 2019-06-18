import com.vividsolutions.jts.geom.{Point, Geometry, GeometryFactory, Coordinate, Envelope, Polygon, LinearRing}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD}
import org.datasyslab.geospark.spatialPartitioning.{FlatGridPartitioner, GridPartitioner}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import java.io._

object FF_Scaleup{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val precision: Double = 0.001
  private var MF_Partitioner: GridPartitioner = new GridPartitioner(List.empty[Envelope].asJava)
  private var FF_Partitioner: GridPartitioner = new GridPartitioner(List.empty[Envelope].asJava)
  private var tag: String = "-1"
  private var appID: String = "app-00000000000000-0000"
  private var startTime: Long = clocktime
  private var cores: Int = 0
  private var executors: Int = 0
  private var portUI: String = "4040"

  case class Flock(pids: List[Int], start: Int, end: Int, center: Point)

  /* SpatialJoin variant */
  def runSpatialJoin(spark: SparkSession, pointset: HashMap[Int, PointRDD], params: FFConf): RDD[Flock] = {
    var clockTime = System.currentTimeMillis()
    cores         = params.cores()
    executors     = params.executors()
    val debug        = params.ffdebug()
    val master       = params.master()
    val sespg        = params.sespg()
    val tespg        = params.tespg()
    val distance     = params.distance()
    val Dpartitions  = (cores * executors) * params.dpartitions()
    val FFpartitions = params.ffpartitions()
    val epsilon      = params.epsilon()
    val mu           = params.mu()
    val delta        = params.delta()
    val spatial      = params.spatial()
    val gridType     = spatial  match {
      case "QUADTREE"  => GridType.QUADTREE
      case "RTREE"     => GridType.RTREE
      case "EQUALGRID" => GridType.EQUALGRID
      case "KDBTREE"   => GridType.KDBTREE
      case "HILBERT"   => GridType.HILBERT
      case "VORONOI"   => GridType.VORONOI
    }
    import spark.implicits._

    // Maximal disks timestamp i...
    var timer = System.currentTimeMillis()
    var stage = ""
    var firstRun: Boolean = true
    var report: RDD[Flock] = spark.sparkContext.emptyRDD[Flock]
    var F: PointRDD = new PointRDD(spark.sparkContext.emptyRDD[Point].toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
    F.analyze()
    for(timestamp <- pointset.keys.toList.sorted){
      // Finding maximal disks...
      tag = s"$timestamp"
      timer = System.currentTimeMillis()
      stage = "1.Maximal disks found"
      logStart(stage)
      val T_i = pointset.get(timestamp).get
      val C = new PointRDD(MF.run(spark, T_i, MF_Partitioner, params, s"$timestamp").map(c => makePoint(c, timestamp)).toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
      val nDisks = C.rawSpatialRDD.count()
      logEnd(stage, timer, nDisks, s"$timestamp")

      if(firstRun){
        F = C
        firstRun = false
      } else {
        // Doing join...
        timer = System.currentTimeMillis()
        stage = "2.Join done"
        logStart(stage)

        F.analyze()
        F.spatialPartitioning(gridType, Dpartitions)
        F.buildIndex(IndexType.QUADTREE, true) // Set to TRUE if run join query...
        val buffers = new CircleRDD(C, distance)
        buffers.spatialPartitioning(F.getPartitioner)

        val R = JoinQuery.DistanceJoinQueryWithDuplicates(F, buffers, true, false) // using index, only return geometries fully covered by each buffer...
        var flocks0 = R.rdd.flatMap{ case (g: Geometry, h: java.util.HashSet[Point]) =>
          val f0 = getFlocksFromGeom(g)
          h.asScala.map{ point =>
            val f1 = getFlocksFromGeom(point)
            (f0, f1)
          }
        }

        if(debug) logger.info(s"FF,Candidates_after_Join,${epsilon},${timestamp},${flocks0.count()}")
        if(debug){
          val t1 = F.rawSpatialRDD.rdd.map{f =>
            val x = f.getX
            val y = f.getY
            val userdata = f.getUserData.toString()
            s"$x\t$y\t$userdata\n"
          }
          val Ffilename = s"F${executors}_${timestamp}.tsv"
          val pw1 = new PrintWriter(Ffilename)
          pw1.write(t1.collect().mkString(""))
          pw1.close()
          val t2 = C.rawSpatialRDD.rdd.map{c =>
            val x = c.getX
            val y = c.getY
            val userdata = c.getUserData.toString()
            s"$x\t$y\t$userdata\n"
          }
          val Cfilename = s"C${executors}_${timestamp}.tsv"
          val pw2 = new PrintWriter(Cfilename)
          pw2.write(t2.collect().mkString(""))
          pw2.close()
          logger.info(s"$Ffilename and $Cfilename have been written...")
        }

        var flocks = flocks0.map{ flocks =>
          val f = flocks._1.pids.intersect(flocks._2.pids)
          val s = flocks._2.start
          val e = flocks._1.end
          val c = flocks._1.center
          Flock(f, s, e, c)
        }.filter(_.pids.length >= mu)
          .map(f => (s"${f.pids};${f.start};${f.end}", f))
          .reduceByKey( (f0, f1) => f0).map(_._2)
          .persist()
        val nFlocks = flocks.count()
        logEnd(stage, timer, nFlocks, s"$timestamp")

        if(debug) logger.info(s"FF,Less_Than_Mu,${epsilon},${timestamp},${nFlocks}")

        // Reporting flocks...
        timer = System.currentTimeMillis()
        stage = "3.Flocks to report"
        logStart(stage)
        var f0 =  flocks.filter{f => f.end - f.start + 1 >= delta}
          .persist(StorageLevel.MEMORY_ONLY)
        var nF0 = f0.count()
        val f1 =  flocks.filter{f => f.end - f.start + 1 <  delta}
          .persist(StorageLevel.MEMORY_ONLY)
        val nF1 = f1.count()
        val G_prime = new PointRDD(
          f0.map{ f =>
            f.center.setUserData(f.pids.mkString(" ") ++ s";${f.start};${f.end}")
            f.center
          }.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg
        )
        G_prime.analyze()
        val nG_prime = G_prime.rawSpatialRDD.count()
        logEnd(stage, timer, nG_prime, s"$timestamp")

        // Prunning flocks by expansions...
        timer = System.currentTimeMillis()
        stage = "4.Flocks prunned by expansion"
        logStart(stage)
        var f0_prime: RDD[String] = spark.sparkContext.emptyRDD[String]
        if(G_prime.boundary() != null){
          f0_prime = pruneFlockByExpansions(G_prime, epsilon, timestamp, spark, params).persist(StorageLevel.MEMORY_ONLY)
        }
        val nF0_prime = f0_prime.count()
        logEnd(stage, timer, nF0_prime, s"$timestamp")

        // Reporting flocks...
        timer = System.currentTimeMillis()
        stage = "5.Flocks reported"
        logStart(stage)
        f0 = f0_prime.map{ f =>
            val arr = f.split(";")
            val p = arr(0).split(" ").map(_.toInt).toList
            val s = arr(1).toInt
            val e = arr(2).toInt
            val x = arr(3).toDouble
            val y = arr(4).toDouble
            val c = geofactory.createPoint(new Coordinate(x, y))
            Flock(p,s,e,c)
        }.persist(StorageLevel.MEMORY_ONLY)
        nF0 = f0.count()
        report = report.union(f0).persist(StorageLevel.MEMORY_ONLY)
        logEnd(stage, timer, nF0, s"$timestamp")

        if(debug) logger.info(s"FF,Flocks_being_reported,${epsilon},${timestamp},${f0.count()}")

        // Indexing candidates...
        timer = System.currentTimeMillis()
        stage = "6.Candidates indexed"
        logStart(stage)
        flocks = f0.map(f => Flock(f.pids, f.start + 1, f.end, f.center))
          .union(f1)
          .union(C.rawSpatialRDD.rdd.map(c => getFlocksFromGeom(c)))
          .map(f => (f.pids, f))
          .reduceByKey( (a,b) => if(a.start < b.start) a else b )
          .map(_._2)
          .distinct()
          .persist(StorageLevel.MEMORY_ONLY)
        F = new PointRDD(flocks.map{ f =>
            val info = s"${f.pids.mkString(" ")};${f.start};${f.end}"
            f.center.setUserData(info)
            f.center
          }.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
        logEnd(stage, timer, F.rawSpatialRDD.count(), s"$timestamp")

        if(debug) logger.info(s"FF,New_set_of_candidates,${epsilon},${timestamp},${F.rawSpatialRDD.count()}")
      }
    }
    logger.info(s"Number of flocks: ${report.count()}")
    val executionTime = "%.2f".format((System.currentTimeMillis() - clockTime) / 1000.0)
    val applicationID = spark.sparkContext.applicationId
    val nReport = report.count()
    logger.info(s"PFLOCK|$applicationID|$cores|$executors|$epsilon|$mu|$delta|$executionTime|$nReport")
    report
  }

  def getFlocksFromGeom(g: Geometry): Flock = {
    val farr = g.getUserData.toString().split(";")
    val pids = farr(0).split(" ").map(_.toInt).toList
    val start = farr(1).toInt
    val end = farr(2).toInt
    val center = g.getCentroid

    Flock(pids, start, end, center)
  }

  def extractTimestamp(points: PointRDD, timestamp:  Int, sespg: String, tespg: String): PointRDD = {
    new PointRDD(points.rawSpatialRDD.rdd.filter{
      _.getUserData().toString().split("\t").reverse.head.toInt == timestamp
    }.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
  }

  def makePoint(pattern: String, timestamp: Int): Point = {
    val arr = pattern.split("\t")
    val point = geofactory.createPoint(new Coordinate(arr(1).toDouble, arr(2).toDouble))
    point.setUserData(s"${arr(0)};${timestamp};${timestamp}")
    point
  }

  import org.apache.spark.Partitioner
  import SPMF.{AlgoLCM2, Transactions, Transaction}

  class ExpansionPartitioner(partitions: Int) extends Partitioner{
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      key.asInstanceOf[Int]
    }
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

  def pruneFlockByExpansions(F: PointRDD, epsilon: Double, timestamp: Int, spark: SparkSession, params: FFConf): RDD[String] = {
    import spark.implicits._
    val debug = params.ffdebug()

    var timer = System.currentTimeMillis()
    var stage = "4a.Making expansions"
    logStart(stage)

    val r = epsilon / 2.0 + precision
    val FCircles = new CircleRDD(F, r)
    FCircles.spatialPartitioning(FF_Partitioner)
    FCircles.spatialPartitionedRDD.cache()
    val nFCircles = FCircles.spatialPartitionedRDD.rdd.count()
    val grids = FF_Partitioner.getGrids.asScala.toList.zipWithIndex.map(g => g._2 -> g._1).toMap
    logEnd(stage, timer, nFCircles, s"$timestamp")

    timer = System.currentTimeMillis()
    stage = "4b.Finding maximals"
    logStart(stage)
    val P = FCircles.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (i, disks) =>
      val transactions = disks.map{ d =>
        val x = d.getCenterPoint.x
        val y = d.getCenterPoint.y
        val arr = d.getUserData.toString().split(";")
        val pids = arr(0)
        val s = arr(1).toInt
        val e = arr(2).toInt
        new Transaction(x, y, s, e, pids)
      }.toList.asJava
      val LCM = new AlgoLCM2()
      val data = new Transactions(transactions, 0)
      LCM.run(data)
      val result = LCM.getPointsAndPids.asScala
         .map{ p =>
           val pids = p.getItems.mkString(" ")
           val grid = grids(i)
           val point = geofactory.createPoint(new Coordinate(p.getX, p.getY))

           val flag = isNotInExpansionArea(point, grid, 0.0)
           ((pids, p.getStart, p.getEnd, p.getX, p.getY),  flag)
         }
         .filter(_._2).map(_._1)
         .map(p => s"${p._1};${p._2};${p._3};${p._4};${p._5}").toList
       result.toIterator
    }.persist(StorageLevel.MEMORY_ONLY)
    val nP = P.count()
    logEnd(stage, timer, nP, s"$timestamp")

    P
  }

  def clocktime = System.currentTimeMillis()

  def logStart(msg: String): Unit ={
    val duration = (clocktime - startTime) / 1000.0
    logger.info("FF|%-30s|%6.2f|%-50s|%6.2f|%6d|%s".format(s"$appID|$executors|$cores|START", duration, msg, 0.0, 0, tag))
  }

  def logEnd(msg: String, timer: Long, n: Long = -1, tag: String = "-1"): Unit ={
    val duration = (clocktime - startTime) / 1000.0
    logger.info("FF|%-30s|%6.2f|%-50s|%6.2f|%6d|%s".format(s"$appID|$executors|$cores|  END", duration, msg, (System.currentTimeMillis()-timer)/1000.0, n, tag))
  }

  import Numeric.Implicits._
  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)
    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

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

  def roundAt(p: Int)(n: Double): Double = { val s = math pow (10, p); (math round n * s) / s }

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def getPartitionerByCellNumber(boundary: Envelope, epsilon: Double, x: Double, y: Double): GridPartitioner = {
    val dx = boundary.getWidth / x
    val dy = boundary.getHeight / y
    getPartitionerByCellSize(boundary, epsilon, dx, dy)
  }

  def getPartitionerByCellSize(boundary: Envelope, epsilon: Double, dx: Double, dy: Double): GridPartitioner = {
    val minx = boundary.getMinX
    val miny = boundary.getMinY
    val maxx = boundary.getMaxX
    val maxy = boundary.getMaxY
    val Xs = (minx until maxx by dx).map(x => roundAt(3)(x))
    val Ys = (miny until maxy by dy).map(y => roundAt(3)(y))
    val g = Xs cross Ys
    val error = 0.0000001
    val grids = g.toList.map(g => new Envelope(g._1, g._1 + dx - error, g._2, g._2 + dy - error))
    logger.info(s"Number of grid envelops: ${grids.size}")
    new GridPartitioner(grids.asJava, epsilon, dx, dy, Xs.size, Ys.size)
  }

  /**************************************
   * The main function...   
   *************************************/
  def main(args: Array[String]): Unit = {
    val params       = new FFConf(args)
    val master       = params.master()
    val port         = params.port()
    val input        = params.input()
    val offset       = params.offset()
    val epsilon      = params.epsilon()
    val fftimestamp  = params.fftimestamp()
    val debug        = params.ffdebug()
    val info         = params.info()
    cores            = params.cores()
    executors        = params.executors()
    portUI           = params.portui()
    val Dpartitions  = (cores * executors) * params.dpartitions()
    
    // Starting session...
    var timer = System.currentTimeMillis()
    var stage = "Session start"
    logStart(stage)
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * cores * executors)
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.cores.max", cores * executors)
      .config("spark.executor.cores", cores)
      .master(s"spark://${master}:${port}")
      .appName("PFLock")
      .getOrCreate()
    import spark.implicits._
    appID = spark.sparkContext.applicationId
    startTime = spark.sparkContext.startTime
    logEnd(stage, timer, 0, "-1")

    // Reading data...
    timer = System.currentTimeMillis()
    stage = "Data read"
    logStart(stage)
    val points = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true, Dpartitions)
    points.CRSTransform(params.sespg(), params.tespg())
    points.analyze()
    val fullBoundary = points.boundary()
    val mf_x = params.mfcustomx()
    val mf_y = params.mfcustomy()
    MF_Partitioner = getPartitionerByCellNumber(fullBoundary, epsilon, mf_x, mf_y)
    val ff_x = params.ffcustomx()
    val ff_y = params.ffcustomy()
    FF_Partitioner = getPartitionerByCellNumber(fullBoundary, epsilon, ff_x, ff_y)
    val nPoints = points.rawSpatialRDD.count()
    val timestamps = points.rawSpatialRDD.rdd.map(_.getUserData.toString().split("\t").reverse.head.toInt).distinct.collect()
    val pointset: HashMap[Int, PointRDD] = new HashMap()
    for(timestamp <- timestamps.filter(_ < fftimestamp)){
      pointset += (timestamp -> extractTimestamp(points, timestamp, params.sespg(), params.tespg()))
    }
    logEnd(stage, timer, nPoints)

    if(debug){ logger.info(s"Current timestamps: ${timestamps.filter(_ < fftimestamp).mkString(" ")}") }

    // Running maximal finder...
    timer = System.currentTimeMillis()
    stage = "Flock Finder run"
    logStart(stage)
    val flocks = runSpatialJoin(spark: SparkSession, pointset: HashMap[Int, PointRDD], params: FFConf)
    logEnd(stage, timer, flocks.count(), tag)

    if(debug){
      val f = flocks.map(f => s"${f.start}, ${f.end}, ${f.pids.mkString(" ")}\n").sortBy(_.toString()).collect().mkString("")
      val pw = new PrintWriter(new File("/tmp/flocks.txt"))
      pw.write(f)
      pw.close
    }

    // Closing session...
    timer = clocktime
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
      f.write(InfoTracker.getTasksInfo())
      f.close()
    }
    spark.close()
    logEnd(stage, timer, 0, tag)    
  }
}
