import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Geometry, Coordinate, Envelope, Polygon, Point}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.spatialPartitioning.{FlatGridPartitioner}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialPartitioning.{SpatialPartitioner}
import org.datasyslab.geospark.spatialPartitioning.quadtree.{QuadTreePartitioner, StandardQuadTree, QuadRectangle}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import java.io.PrintWriter
import org.apache.spark.TaskContext

object FF{
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val model: PrecisionModel = new PrecisionModel(1000)
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val reader = new com.vividsolutions.jts.io.WKTReader(geofactory)
  private val precision: Double = 0.001
  private var tag: String = "-1"
  private var appID: String = "app-00000000000000-0000"
  private var startTime: Long = clocktime
  private var cores: Int = 0
  private var executors: Int = 0
  private var portUI: String = "4040"
  private var filenames: ArrayBuffer[String] = new ArrayBuffer()

  def run(spark: SparkSession, timestamps: List[Int], params: FFConf): Unit = {
    val applicationID = spark.sparkContext.applicationId
    var clockTime = System.currentTimeMillis()
    val debug        = params.debug()
    val master       = params.master()
    val offset       = params.offset()
    val sespg        = params.sespg()
    val tespg        = params.tespg()
    val distance     = params.distance()
    val Dpartitions  = params.dpartitions()
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
    //var firstRun: Boolean = true
    var flocks: RDD[Flock] = spark.sparkContext.emptyRDD[Flock]
    var nFlocks: Long = 0L
    var lastFlocks: RDD[Flock] = null
    var lastC: SpatialRDD[Point] = null
    var nReported = 0
    for(timestamp <- timestamps){ //for
      // Finding maximal disks...
      val input = s"${params.input_path()}${params.input_tag()}_${timestamp}.tsv"
      val T_i = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true, Dpartitions)

      //////////////////////////////////////////////////////////
      // Getting maximal disks...
      //////////////////////////////////////////////////////////
      timer = System.currentTimeMillis()
      stage = "1.Getting maximal disks"
      logStart(stage, timestamp)
      T_i.analyze()
      val (disks, nDisks) = MF.run(spark, T_i, params, timestamp, s"$timestamp")
      val C = new SpatialRDD[Point]()
      C.setRawSpatialRDD(disks)
      if(nDisks == 0){
        lastC == null
      }

      if(lastC != null){ // To control GC performance...
        lastC.rawSpatialRDD.unpersist(false)
        lastC.spatialPartitionedRDD.unpersist(false)
      }
      lastC = C
      C.analyze()
      C.spatialPartitioning(gridType, FFpartitions)
      logEnd(stage, timer, nDisks, timestamp)

      flocks = if(nDisks != 0){
        if(flocks.isEmpty())
          C.rawSpatialRDD.rdd.map( geom2flock )
        else
          flocks
      } else {
        lastFlocks = null
        lastC = null
        spark.sparkContext.emptyRDD[Flock]
      }  

      if(!flocks.isEmpty()){
        // Doing join...
        val timerJoinAndReport = System.currentTimeMillis()

        //////////////////////////////////////////////////////////
        // Join done...
        //////////////////////////////////////////////////////////
        timer = System.currentTimeMillis()
        stage = "2.Join done"
        logStart(stage, timestamp)
        val F = new SpatialRDD[Point]()
        val flockPoints = flocks.map{ f =>  // Create spatial RDD with candidate flocks.
          val userData = s"${f.getItems.mkString(" ")};${f.start};${f.end}"
          f.center.setUserData(userData)
          f.center
        }
        F.setRawSpatialRDD(flockPoints)
        F.analyze()
        F.spatialPartitioning(gridType, FFpartitions)
        F.spatialPartitionedRDD.rdd.cache
        F.buildIndex(IndexType.QUADTREE, true) // Set to TRUE if run join query...
        val disks = new CircleRDD(C, distance)
        disks.spatialPartitioning(F.getPartitioner)

        if(debug){
          logger.info(s"Candidate flocks ($nFlocks)")
          val nF = F.spatialPartitionedRDD.rdd.count()
          val nDisks = disks.rawSpatialRDD.count()
          logger.info(s"Join between F ($nF) and buffers ($nDisks)")
        }

        val usingIndex = true 
        val fullyCovered = false // only return geometries fully covered by each buffer...
        val R = JoinQuery.DistanceJoinQuery(F, disks, usingIndex, fullyCovered)

        val flocks0 = R.rdd.map{ case (disk: Geometry, flocks: java.util.HashSet[Point]) =>
          val flock1 = geom2flock(disk)
          val f =  flocks.asScala.map{ flock =>
            val flock2 = geom2flock(flock)
            val i = flock1.getItems.intersect(flock2.getItems)
            val s = flock2.start
            val e = flock1.end
            val c = flock1.center
            Flock(i, s, e, c)
          }.filter(_.size >= mu)
            .map(f => (s"${f.getItems};${f.start};${f.end}", f))
            .groupBy(_._1).mapValues(_.head._2).values

          f
        }

        if(debug){
          /*
          val g0 = R.rdd.map{ case (disk: Geometry, flocks: java.util.HashSet[Point]) =>
            val flock1 = geom2flock(disk)
            flocks.asScala.map{ d =>
              val flock2 = geom2flock(d)
              val i = flock1.getItems.intersect(flock2.getItems)
              val s = flock2.start
              val e = flock1.end
              val c = flock1.center
              (flock1, flock2, Flock(i, s, e, c))
            }.filter(_._3.size >= mu)
          }
          g0.map { f =>
            f.filter(t => Set(40,53,56).subsetOf(t._3.getItemset))
              .map(t => s"${t._1.toTSV()} =VS= ${t._2.toTSV()} => ${t._3.toTSV()}")
          }.foreach{ println }
          logger.info(s"Join: ${g0.count()}")
           

          val g1 = flocks0.zipWithIndex().flatMap{ case (flocks, i) =>
            flocks.toVector
              .map(f => (i,f)).sortBy(_._1)
              .map{ case(i, flock) => s"${i}\t${flock.toTSV()}" }
          }

          g1.foreach{ println }
          logger.info(s"Initial: ${g1.count()}")

          val g2 = flocks0.zipWithIndex().flatMap{ case (flocks, i) =>
            val subsets = for{
              f1 <- flocks
              f2 <- flocks if f1.size < f2.size
            } yield {
              val isSubset = f1.getItemset.subsetOf(f2.getItemset) && f1.start >= f2.start
              (f1, f2, isSubset)
            }
            subsets.filter(_._3).map(_._1).toVector.distinct
              .map(f => (i,f)).sortBy(_._1)
              .map{ case(i, flock) => s"${i}\t${flock.toTSV()}" }
          }

          g2.foreach{ println }
          logger.info(s"Remove: ${g2.count()}")
           */
        }

        val flocks1 = flocks0.map{ flocks =>
            val subsets = for{
              f1 <- flocks
              f2 <- flocks if f1.size < f2.size
            } yield {
              val isSubset = f1.getItemset.subsetOf(f2.getItemset) && f1.start >= f2.start
              (f1, f2, isSubset)
            }
            subsets.filter(_._3).map(_._1).toVector.distinct
        }

        flocks = flocks0.flatMap(f => f).subtract(flocks1.flatMap(f => f))

        if(debug){
          //flocks.map(_.toTSV()).foreach{ println }
          //logger.info(s"Final: ${flocks.count()}")
        }

        nFlocks = flocks.count()
        logEnd(stage, timer, nFlocks, timestamp)

        //////////////////////////////////////////////////////////
        // Flocks reported...
        //////////////////////////////////////////////////////////
        timer = System.currentTimeMillis()
        stage = "3.Flocks reported"
        logStart(stage, timestamp)
        val flocks_delta =  flocks.filter(_.length == delta).cache
        val nFlocks_delta = flocks_delta.count().toInt

        val partitioner = F.getPartitioner
        val flocks_to_report = getRedundants(flocks_delta, partitioner, epsilon, spark, params).cache
        val nFlocks_to_report = flocks_to_report.count().toInt
        logEnd(stage, timer, nFlocks_to_report, timestamp)

        // Saving partial results...
        val n = saveFlocks(flocks_to_report, timestamp)
        nReported = nReported + n 

        //////////////////////////////////////////////////////////
        // Flocks updateed...
        //////////////////////////////////////////////////////////
        timer = System.currentTimeMillis()
        stage = "4.Flocks updated"
        logStart(stage, timestamp)
        val flocks_updated = flocks_to_report.map{ f =>
          f.copy(start = f.start + 1)
        }.cache
        val nFlocks_updated = flocks_updated.count().toInt
        val previous_flocks = flocks_updated.union(flocks.filter(_.length < delta)).cache
        val nPrevious_flocks = previous_flocks.count().toInt
        val new_flocks = disks.getCenterPointAsSpatialRDD
          .getRawSpatialRDD.rdd.map{ geom2flock }.cache
        val nNew_flocks= new_flocks.count().toInt

        val flocks_to_prune = getFlocksToPrune(previous_flocks, disks, spark).cache
        val nFlocks_to_prune = flocks_to_prune.count().toInt
        val flocks_pruned = new_flocks.subtract(flocks_to_prune).cache
        val nFlocks_pruned = flocks_pruned.count().toInt
        flocks = previous_flocks.union(flocks_pruned).cache
        nFlocks = flocks.count()
        logEnd(stage, timer, nFlocks, timestamp)
      }
    } // rof
    logger.info(s"Number of flocks: ${nReported}")
    val executionTime = "%.2f".format((System.currentTimeMillis() - clockTime) / 1000.0)
    logger.info(s"PFLOCK|$applicationID|$cores|$executors|$epsilon|$mu|$delta|$executionTime|$nReported")
    if(params.save()){ 
      val txtFlocks = filenames.flatMap{ filename =>
        scala.io.Source.fromFile(filename).getLines
      }
      val output = s"/tmp/FF_E${epsilon.toInt}_M${mu}_D${delta}.tsv"
      val f = new java.io.PrintWriter(output)
      f.write(txtFlocks.mkString("\n"))
      f.write("\n")
      f.close()
      logger.info(s"Saved $output [${txtFlocks.size} flocks].")
    }
  }

  def saveFlocks(flocks: RDD[Flock], instant: Int): Int = {
    if(flocks.count() > 0){
      val filename = s"/tmp/joinFlocks_${instant}.tsv"
      val fw = new java.io.FileWriter(filename)
      val out = flocks.map(f => s"${f.start}, ${f.end}, ${f.getItems.mkString(" ")}\n")
        .collect()
      fw.write(out.mkString(""))
      fw.close()
      logger.info(s"Flocks saved at $filename [${out.size} records]")
      filenames += filename
      out.size
    } else {
      0
    }
  }

  def saveFlocks2(flocks: RDD[Flock], instant: Int): Int = {
    if(flocks.count() > 0){
      val filename = s"/tmp/joinFlocks_${instant}.tsv"
      val fw = new java.io.FileWriter(filename)
      val out = flocks.map(f => s"${f.start}, ${f.end}, ${f.getItems.mkString(" ")}\n").distinct
        .collect()
      fw.write(out.mkString(""))
      fw.close()
      logger.info(s"Flocks saved at $filename [${out.size} records]")
      filenames += filename
      out.size
    } else {
      0
    }
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

  def getFlocksToPrune(flocksRDD: RDD[Flock], disks: CircleRDD, spark: SparkSession, debug: Boolean = false): RDD[Flock] = {
    import spark.implicits._
    val flockPoints = flocksRDD.map{ flock => 
      val point = flock.center
      point.setUserData(s"${flock.getItems.mkString(" ")};${flock.start};${flock.end}")
      point
    }
    val flocks = new SpatialRDD[Point]()
    flocks.setRawSpatialRDD(flockPoints)
    flocks.analyze()
    flocks.spatialPartitioning(disks.getPartitioner)
    flocks.buildIndex(IndexType.QUADTREE, true) 
    val F = JoinQuery.DistanceJoinQueryFlat(flocks, disks, true, false)
    val flocks_prime = F.rdd.map{ case (disk: Point, flock: Point) =>
      (disk, flock)
    }.groupByKey()

    val f = flocks_prime.flatMap{ entry =>
      val disk  = geom2flock(entry._1)
      val flocks = entry._2.map(geom2flock)
      flocks.map{ flock =>
        val subset = disk.getItemset.subsetOf(flock.getItemset)
        (disk, flock, subset)
      }
    }

    if(debug){
      f.map{ f => (f._1.toString(), f._2.toString(), f._3)  }.toDS().show(100, false)
    }

    f.filter(_._3).map(_._1)
  }

  def geom2flock(geom: Geometry): Flock = {
    val farr   = geom.getUserData.toString().split(";")
    val items  = farr(0).split(" ").map(_.toInt).toVector
    val start  = farr(1).toInt
    val end    = farr(2).toInt
    val center = geom.getCentroid

    Flock(items, start, end, center)
  }

  def flock2point(flock: Flock): Point = {
    val point = flock.center
    point.setUserData(s"${flock.getItems.mkString(" ")};${flock.start};${flock.end}")
    point
  }

  def flockRDD2pointRDD(flockRDD: RDD[Flock]): (SpatialRDD[Point], Long) = {
    val points = flockRDD.map(flock2point)
    val nPoints = points.count().toInt
    val pointRDD = new SpatialRDD[Point]()
    pointRDD.setRawSpatialRDD(points)
    pointRDD.analyze()
    (pointRDD, points.count())
  }

  def pruneDuplicates(flocks: RDD[Flock]): RDD[Flock] = {
    flocks.map(f => ((f.start, f.end, f.items), f)).groupByKey().mapValues(_.head).values
  }

  def getRedundants(flocks: RDD[Flock], partitioner: SpatialPartitioner,epsilon: Double, spark: SparkSession, params: FFConf, debug: Boolean = false): RDD[Flock] = {
    if(flocks.isEmpty()){
      flocks
    } else {
      import spark.implicits._
      val (pointRDD, n) = flockRDD2pointRDD(flocks)
      pointRDD.spatialPartitioning(partitioner)
      val bufferRDD = new CircleRDD(pointRDD, epsilon)
      bufferRDD.spatialPartitioning(pointRDD.getPartitioner)
      if(params.debug()){
        logger.info(s"Number of partition in getRedundants: ${pointRDD.spatialPartitionedRDD.rdd.getNumPartitions}")
      }
      val F = JoinQuery.DistanceJoinQueryFlat(pointRDD, bufferRDD, false, false)
      val flocks_prime = F.rdd
        .map{ case (point1, point2) => (geom2flock(point1), geom2flock(point2)) }
        .filter{ case(flock1, flock2) => flock1.size < flock2.size }
        .map{ case(flock1, flock2) =>
          val subset = flock1.getItemset.subsetOf(flock2.getItemset)
          (flock1, flock2, subset)
        }
      
      if(debug){
        flocks_prime.map(f => (f._1.toString(), f._2.toString(), f._3)).toDS().show(5, false)
      }

      pruneDuplicates(flocks.subtract(flocks_prime.filter(_._3).map(_._1)))
    }
  }

  def clocktime = System.currentTimeMillis()

  def logStart(msg: String, t: Int): Unit ={
    val duration = (clocktime - startTime) / 1000.0
    val time = 0.0
    val n = 0
    val log = f"FF|START|$appID%s|$executors%d|$cores%d|$duration%6.2f|$msg%-30s|$time%6.2f|$n%6d|$t"
    logger.info(log)
  }

  def logEnd(msg: String, timer: Long, n: Long, t: Int): Unit ={
    val duration = (clocktime - startTime) / 1000.0
    val time = (clocktime - timer) / 1000.0
    val log = f"FF|  END|$appID%s|$executors%d|$cores%d|$duration%6.2f|$msg%-30s|$time%6.2f|$n%6d|$t"
    logger.info(log)
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
    val coords = Array(p1,p2,p3,p4,p1)
    geofactory.createPolygon(coords)
  }

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def readGridCell(wkt: String): Envelope = {
    reader.read(wkt).getEnvelopeInternal
  }

  /**************************************
   * The main function...   
   *************************************/
  def main(args: Array[String]): Unit = {
    val params       = new FFConf(args)
    val master       = params.master()
    val port         = params.port()
    val input        = params.input()
    val checkpointDir= params.check_dir()
    val m_grid       = params.m_grid()
    val p_grid       = params.p_grid()
    val offset       = params.offset()
    val epsilon      = params.epsilon()
    val mininterval  = params.mininterval()
    val maxinterval  = params.maxinterval()
    val debug        = params.debug()
    val info         = params.info()
    cores            = params.cores()
    executors        = params.executors()
    portUI           = params.portui()
    val Mpartitions  = params.mfpartitions()
    val Dpartitions  = (cores * executors) * params.dpartitions()
    
    // Starting session...
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * cores * executors)
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.executor.cores", cores)
      .config("spark.cores.max", executors * cores)
      .appName("PFLock")
      .getOrCreate()
    import spark.implicits._
    appID = spark.sparkContext.applicationId.split("-").last
    startTime = spark.sparkContext.startTime
    val config = spark.sparkContext.getConf.getAll.mkString("\n")
    logger.info(config)

    // Running maximal finder...
    var timestamps = (mininterval to maxinterval).toList
    if(params.tsfile() != ""){
      val ts = scala.io.Source.fromFile(params.tsfile())
      timestamps =  ts.getLines.map(_.toInt).toList
      ts.close
    }

    logger.info("Reading data file by file...")
    run(spark, timestamps, params)

    // Closing session...
    logger.info("Closing session...")
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
    logger.info("Closing session... Done!")
  }
}
