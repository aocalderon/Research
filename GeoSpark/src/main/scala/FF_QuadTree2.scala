import com.vividsolutions.jts.geom.{Point, Geometry, GeometryFactory, Coordinate, Envelope, Polygon, LinearRing}
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
import org.datasyslab.geospark.spatialPartitioning.{KDBTree, KDBTreePartitioner}
import org.datasyslab.geospark.spatialPartitioning.quadtree.{QuadTreePartitioner, StandardQuadTree, QuadRectangle}
import org.datasyslab.geospark.utils.RDDSampleUtils
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.io.Source
import java.io._

object FF_QuadTree2{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val reader = new com.vividsolutions.jts.io.WKTReader(geofactory)
  private val precision: Double = 0.001
  private var tag: String = "-1"
  private var appID: String = "app-00000000000000-0000"
  private var startTime: Long = clocktime
  private var cores: Int = 0
  private var executors: Int = 0
  private var portUI: String = "4040"

  case class Flock(pids: List[Int], start: Int, end: Int, center: Point)

  //def run(spark: SparkSession, points: PointRDD, timestamps: List[Int], QTPartitioner: QuadTreePartitioner, params: FFConf): RDD[Flock] = {
  def run(spark: SparkSession, timestamps: List[Int], params: FFConf): Unit = {
    val applicationID = spark.sparkContext.applicationId
    var clockTime = System.currentTimeMillis()
    cores         = params.cores()
    executors     = params.executors()
    val debug        = params.ffdebug()
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
    var firstRun: Boolean = true
    var flocks: RDD[Flock] = spark.sparkContext.emptyRDD[Flock]
    var lastFlocks: RDD[Flock] = null
    var lastC: PointRDD = null
    var nF0 = 0
    for(timestamp <- timestamps){ //for
      // Finding maximal disks...
      tag = s"$timestamp"
      val input = s"${params.input_path()}${params.input_tag()}_${timestamp}.tsv"
      val T_i = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true, Dpartitions)
      timer = System.currentTimeMillis()
      stage = "1.Maximal disks found"
      logStart(stage)
      T_i.analyze()
      T_i.CRSTransform(sespg, tespg)
      val MFrun = MF_QuadTree3.run(spark, T_i, params, timestamp, s"$timestamp")
      val C = MFrun._1
      val nDisks = MFrun._2
      if(nDisks == 0){
        firstRun = true
        lastC == null
      }

      if(lastC != null){ // To control GC performance...
        lastC.rawSpatialRDD.unpersist(false)
        lastC.spatialPartitionedRDD.unpersist(false)
      }
      lastC = C
      C.analyze()
      C.CRSTransform(sespg, tespg)
      logEnd(stage, timer, nDisks, s"$timestamp")

      if(firstRun){
        if(nDisks != 0){
          flocks = C.rawSpatialRDD.rdd.map(c => getFlocksFromGeom(c))
          firstRun = false
        } else {
          flocks = spark.sparkContext.emptyRDD[Flock]
          firstRun = true
          lastFlocks = null
          lastC = null
        }
      } else {
        // Doing join...
        val timerJoinAndReport = System.currentTimeMillis()
        timer = System.currentTimeMillis()
        stage = "2.Join done"
        logStart(stage)
        val F: PointRDD = new PointRDD(flocks.map{ f =>  // Create spatial RDD with candidate flocks.
            val info = s"${f.pids.mkString(" ")};${f.start};${f.end}"
            f.center.setUserData(info)
            f.center
        }.toJavaRDD(), StorageLevel.MEMORY_ONLY_SER, sespg, tespg)        
        F.analyze()
        F.spatialPartitioning(C.getPartitioner)
        F.buildIndex(IndexType.QUADTREE, true) // Set to TRUE if run join query...
        val buffers = new CircleRDD(C, distance)
        buffers.spatialPartitioning(C.getPartitioner)

        if(debug){
          val nF = F.rawSpatialRDD.count()
          val nBuffers = buffers.rawSpatialRDD.count()
          logger.info(s"Join between F ($nF) and buffers ($nBuffers)")
        }

        val R = JoinQuery.DistanceJoinQueryWithDuplicates(F, buffers, true, false) // using index, only return geometries fully covered by each buffer...

        flocks = R.rdd.flatMap{ case (g: Geometry, h: java.util.HashSet[Point]) =>
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
        }.filter(_.pids.length >= mu)
          .map(f => (s"${f.pids};${f.start};${f.end}", f))
          .reduceByKey( (f0, f1) => f0).map(_._2)
          .cache()
        val nFlocks = flocks.count()
        logEnd(stage, timer, nFlocks, s"$timestamp")

        if(debug){
          logger.info(s"Flocks from join: $nFlocks")
        }

        // Reporting flocks...
        timer = System.currentTimeMillis()
        stage = "3.1.Flocks to clean >= delta"
        logStart(stage)
        var f0 =  flocks.filter{f => f.end - f.start + 1 >= delta}.cache()
        val G_prime = new PointRDD(
          f0.map{ f =>
            f.center.setUserData(f.pids.mkString(" ") ++ s";${f.start};${f.end}")
            f.center
          }.toJavaRDD(), StorageLevel.MEMORY_ONLY_SER, sespg, tespg
        )
        G_prime.analyze()
        val nG_prime = G_prime.rawSpatialRDD.count()
        logEnd(stage, timer, nG_prime, s"$timestamp")

        // Reporting flocks...
        timer = System.currentTimeMillis()
        stage = "3.2.Flocks to clean < delta"
        logStart(stage)
        var f1 =  flocks.filter{f => f.end - f.start + 1 <  delta}.cache()
        var nF1 = f1.count()
        val H_prime = new PointRDD(
          f1.map{ f =>
            f.center.setUserData(f.pids.mkString(" ") ++ s";${f.start};${f.end}")
            f.center
          }.toJavaRDD(), StorageLevel.MEMORY_ONLY_SER, sespg, tespg
        )
        H_prime.analyze()
        val nH_prime = H_prime.rawSpatialRDD.count()
        logEnd(stage, timer, nH_prime, s"$timestamp")

        if(debug){
          logger.info(s"G Flocks before prune: $nG_prime")
          logger.info(s"H Flocks before prune: $nH_prime")
        }

        // Prunning flocks by expansions...
        timer = System.currentTimeMillis()
        stage = "4.1. G Flocks prunned by expansion"
        logStart(stage)
        var f0_prime: RDD[String] = spark.sparkContext.emptyRDD[String]
        if(G_prime.boundary() != null){
          f0_prime = pruneFlockByExpansions(G_prime, epsilon, timestamp, spark, params).persist(StorageLevel.MEMORY_ONLY_SER)
        }
        G_prime.rawSpatialRDD.unpersist(false)
        val nF0_prime = f0_prime.count()
        logEnd(stage, timer, nF0_prime, s"$timestamp")

        timer = System.currentTimeMillis()
        stage = "4.2. H Flocks prunned by expansion"
        logStart(stage)
        var f1_prime: RDD[String] = spark.sparkContext.emptyRDD[String]
        if(H_prime.boundary() != null){
          f1_prime = pruneFlockByExpansions(H_prime, epsilon, timestamp, spark, params).persist(StorageLevel.MEMORY_ONLY_SER)
        }
        H_prime.rawSpatialRDD.unpersist(false)
        val nF1_prime = f1_prime.count()
        logEnd(stage, timer, nF1_prime, s"$timestamp")

        if(debug){
          logger.info(s"G Flocks after prune: $nF0_prime")
          logger.info(s"H Flocks after prune: $nF1_prime")
        }

        // Reporting flocks...
        timer = System.currentTimeMillis()
        stage = "5.1. G Flocks reported"
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
        }.persist(StorageLevel.MEMORY_ONLY_SER)
        val nnF0 = f0.count().toInt  
        nF0 += nnF0
        // Saving flocks...
        val timerSave = System.currentTimeMillis()
        saveFlocks(f0)
        val SaveTime = (System.currentTimeMillis() - timerSave) / 1000.0
        logEnd(stage, timer, nF0, s"$timestamp")

        timer = System.currentTimeMillis()
        stage = "5.2. H Flocks candidate"
        logStart(stage)
        f1 = f1_prime.map{ f =>
            val arr = f.split(";")
            val p = arr(0).split(" ").map(_.toInt).toList
            val s = arr(1).toInt
            val e = arr(2).toInt
            val x = arr(3).toDouble
            val y = arr(4).toDouble
            val c = geofactory.createPoint(new Coordinate(x, y))
            Flock(p,s,e,c)
        }.persist(StorageLevel.MEMORY_ONLY_SER)
        nF1 = f1.count()
        logEnd(stage, timer, nF1, s"$timestamp")

        if(debug){
          logger.info(s"Flocks reported  at ${timestamp}: ${f0.count()}")
          logger.info(s"Flocks candidate at ${timestamp}: ${f1.count()}")
        }

        // Indexing candidates...
        timer = System.currentTimeMillis()
        stage = "6.Candidates updated"
        logStart(stage)
        flocks = f0.map(f => Flock(f.pids, f.start + 1, f.end, f.center)) // Add flock reported with its start updated.
          .union(f1.filter(_.end == timestamp)) // Add previous flocks which touch current time interval.
          .union(C.rawSpatialRDD.rdd.map(c => getFlocksFromGeom(c))) // Add current maximal disks as flock size 1.
          .map(f => (f.pids, f))
          .reduceByKey( (a,b) => if(a.start < b.start) a else b ) // Prune redundant flocks by time.
          .map(_._2)
          .distinct()
          .persist(StorageLevel.MEMORY_ONLY_SER)
        val JoinAndReportTime = (System.currentTimeMillis() - timerJoinAndReport) / 1000.0
        val JoinTime = "%.2f".format(JoinAndReportTime - SaveTime)
        logger.info(s"JOIN|$applicationID|$cores|$executors|$epsilon|$mu|$delta|$JoinTime|$nnF0")

        //if(timestamp % 10 == 0){
        //  flocks.checkpoint()
        //}
        if(lastFlocks != null){
          lastFlocks.unpersist(false)
        }
        lastFlocks = flocks
        logEnd(stage, timer, F.rawSpatialRDD.count(), s"$timestamp")

        if(debug) logger.info(s"Candidates at ${timestamp}: ${F.rawSpatialRDD.count()}")

        // unpersist RDDs...
        f0_prime.unpersist(false)
        f1_prime.unpersist(false)
        f0.unpersist(false)
        f1.unpersist(false)
      }
    } // rof
    logger.info(s"Number of flocks: ${nF0}")
    val executionTime = "%.2f".format((System.currentTimeMillis() - clockTime) / 1000.0)
    val nReport = nF0
    logger.info(s"PFLOCK|$applicationID|$cores|$executors|$epsilon|$mu|$delta|$executionTime|$nReport")
  }

  case class ST_Point(pid: Int, x: Double, y: Double, t: Int)
  def run2(spark: SparkSession, timestamps: List[Int], data: RDD[ST_Point], params: FFConf): Unit = {
    val applicationID = spark.sparkContext.applicationId
    var clockTime = System.currentTimeMillis()
    cores         = params.cores()
    executors     = params.executors()
    val debug        = params.ffdebug()
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
    var firstRun: Boolean = true
    var flocks: RDD[Flock] = spark.sparkContext.emptyRDD[Flock]
    var lastFlocks: RDD[Flock] = null
    var lastC: PointRDD = null
    var nF0 = 0
    for(timestamp <- timestamps){ //for
      // Finding maximal disks...
      tag = s"$timestamp"
      val T_i = new PointRDD(data.filter(_.t == timestamp).map{ p =>
        val point = geofactory.createPoint(new Coordinate(p.x, p.y))
        point.setUserData(s"${p.pid}\t${p.t}")
        point
      }, StorageLevel.MEMORY_ONLY, params.sespg(), params.tespg())
      timer = System.currentTimeMillis()
      stage = "1.Maximal disks found"
      logStart(stage)
      T_i.analyze()
      T_i.CRSTransform(sespg, tespg)
      val MFrun = MF_QuadTree3.run(spark, T_i, params, timestamp, s"$timestamp")
      val C = MFrun._1
      val nDisks = MFrun._2
      if(nDisks == 0){
        firstRun = true
        lastC == null
      }

      if(lastC != null){ // To control GC performance...
        lastC.rawSpatialRDD.unpersist(false)
        lastC.spatialPartitionedRDD.unpersist(false)
      }
      lastC = C
      C.analyze()
      C.CRSTransform(sespg, tespg)
      logEnd(stage, timer, nDisks, s"$timestamp")

      if(firstRun){
        if(nDisks != 0){
          flocks = C.rawSpatialRDD.rdd.map(c => getFlocksFromGeom(c))
          firstRun = false
        } else {
          flocks = spark.sparkContext.emptyRDD[Flock]
          firstRun = true
          lastFlocks = null
          lastC = null
        }
      } else {
        // Doing join...
        val timerJoinAndReport = System.currentTimeMillis()
        timer = System.currentTimeMillis()
        stage = "2.Join done"
        logStart(stage)
        val F: PointRDD = new PointRDD(flocks.map{ f =>  // Create spatial RDD with candidate flocks.
            val info = s"${f.pids.mkString(" ")};${f.start};${f.end}"
            f.center.setUserData(info)
            f.center
        }.toJavaRDD(), StorageLevel.MEMORY_ONLY_SER, sespg, tespg)        
        F.analyze()
        F.spatialPartitioning(C.getPartitioner)
        F.buildIndex(IndexType.QUADTREE, true) // Set to TRUE if run join query...
        val buffers = new CircleRDD(C, distance)
        buffers.spatialPartitioning(C.getPartitioner)

        if(debug){
          val nF = F.rawSpatialRDD.count()
          val nBuffers = buffers.rawSpatialRDD.count()
          logger.info(s"Join between F ($nF) and buffers ($nBuffers)")
        }

        val R = JoinQuery.DistanceJoinQueryWithDuplicates(F, buffers, true, false) // using index, only return geometries fully covered by each buffer...

        flocks = R.rdd.flatMap{ case (g: Geometry, h: java.util.HashSet[Point]) =>
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
        }.filter(_.pids.length >= mu)
          .map(f => (s"${f.pids};${f.start};${f.end}", f))
          .reduceByKey( (f0, f1) => f0).map(_._2)
          .cache()
        val nFlocks = flocks.count()
        logEnd(stage, timer, nFlocks, s"$timestamp")

        if(debug){
          logger.info(s"Flocks from join: $nFlocks")
        }

        // Reporting flocks...
        timer = System.currentTimeMillis()
        stage = "3.1.Flocks to clean >= delta"
        logStart(stage)
        var f0 =  flocks.filter{f => f.end - f.start + 1 >= delta}.cache()
        val G_prime = new PointRDD(
          f0.map{ f =>
            f.center.setUserData(f.pids.mkString(" ") ++ s";${f.start};${f.end}")
            f.center
          }.toJavaRDD(), StorageLevel.MEMORY_ONLY_SER, sespg, tespg
        )
        G_prime.analyze()
        val nG_prime = G_prime.rawSpatialRDD.count()
        logEnd(stage, timer, nG_prime, s"$timestamp")

        // Reporting flocks...
        timer = System.currentTimeMillis()
        stage = "3.2.Flocks to clean < delta"
        logStart(stage)
        var f1 =  flocks.filter{f => f.end - f.start + 1 <  delta}.cache()
        var nF1 = f1.count()
        val H_prime = new PointRDD(
          f1.map{ f =>
            f.center.setUserData(f.pids.mkString(" ") ++ s";${f.start};${f.end}")
            f.center
          }.toJavaRDD(), StorageLevel.MEMORY_ONLY_SER, sespg, tespg
        )
        H_prime.analyze()
        val nH_prime = H_prime.rawSpatialRDD.count()
        logEnd(stage, timer, nH_prime, s"$timestamp")

        if(debug){
          logger.info(s"G Flocks before prune: $nG_prime")
          logger.info(s"H Flocks before prune: $nH_prime")
        }

        // Prunning flocks by expansions...
        timer = System.currentTimeMillis()
        stage = "4.1. G Flocks prunned by expansion"
        logStart(stage)
        var f0_prime: RDD[String] = spark.sparkContext.emptyRDD[String]
        if(G_prime.boundary() != null){
          f0_prime = pruneFlockByExpansions(G_prime, epsilon, timestamp, spark, params).persist(StorageLevel.MEMORY_ONLY_SER)
        }
        G_prime.rawSpatialRDD.unpersist(false)
        val nF0_prime = f0_prime.count()
        logEnd(stage, timer, nF0_prime, s"$timestamp")

        timer = System.currentTimeMillis()
        stage = "4.2. H Flocks prunned by expansion"
        logStart(stage)
        var f1_prime: RDD[String] = spark.sparkContext.emptyRDD[String]
        if(H_prime.boundary() != null){
          f1_prime = pruneFlockByExpansions(H_prime, epsilon, timestamp, spark, params).persist(StorageLevel.MEMORY_ONLY_SER)
        }
        H_prime.rawSpatialRDD.unpersist(false)
        val nF1_prime = f1_prime.count()
        logEnd(stage, timer, nF1_prime, s"$timestamp")

        if(debug){
          logger.info(s"G Flocks after prune: $nF0_prime")
          logger.info(s"H Flocks after prune: $nF1_prime")
        }

        // Reporting flocks...
        timer = System.currentTimeMillis()
        stage = "5.1. G Flocks reported"
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
        }.persist(StorageLevel.MEMORY_ONLY_SER)
        val nnF0 = f0.count().toInt  
        nF0 += nnF0
        // Saving flocks...
        val timerSave = System.currentTimeMillis()
        saveFlocks(f0)
        val SaveTime = (System.currentTimeMillis() - timerSave) / 1000.0
        logEnd(stage, timer, nF0, s"$timestamp")

        timer = System.currentTimeMillis()
        stage = "5.2. H Flocks candidate"
        logStart(stage)
        f1 = f1_prime.map{ f =>
            val arr = f.split(";")
            val p = arr(0).split(" ").map(_.toInt).toList
            val s = arr(1).toInt
            val e = arr(2).toInt
            val x = arr(3).toDouble
            val y = arr(4).toDouble
            val c = geofactory.createPoint(new Coordinate(x, y))
            Flock(p,s,e,c)
        }.persist(StorageLevel.MEMORY_ONLY_SER)
        nF1 = f1.count()
        logEnd(stage, timer, nF1, s"$timestamp")

        if(debug){
          logger.info(s"Flocks reported  at ${timestamp}: ${f0.count()}")
          logger.info(s"Flocks candidate at ${timestamp}: ${f1.count()}")
        }

        // Indexing candidates...
        timer = System.currentTimeMillis()
        stage = "6.Candidates updated"
        logStart(stage)
        flocks = f0.map(f => Flock(f.pids, f.start + 1, f.end, f.center)) // Add flock reported with its start updated.
          .union(f1.filter(_.end == timestamp)) // Add previous flocks which touch current time interval.
          .union(C.rawSpatialRDD.rdd.map(c => getFlocksFromGeom(c))) // Add current maximal disks as flock size 1.
          .map(f => (f.pids, f))
          .reduceByKey( (a,b) => if(a.start < b.start) a else b ) // Prune redundant flocks by time.
          .map(_._2)
          .distinct()
          .persist(StorageLevel.MEMORY_ONLY_SER)
        val JoinAndReportTime = (System.currentTimeMillis() - timerJoinAndReport) / 1000.0
        val JoinTime = "%.2f".format(JoinAndReportTime - SaveTime)
        logger.info(s"JOIN|$applicationID|$cores|$executors|$epsilon|$mu|$delta|$JoinTime|$nnF0")

        if(timestamp % 10 == 0){
          flocks.checkpoint()
        }
        if(lastFlocks != null){
          lastFlocks.unpersist(false)
        }
        lastFlocks = flocks
        logEnd(stage, timer, F.rawSpatialRDD.count(), s"$timestamp")

        if(debug) logger.info(s"Candidates at ${timestamp}: ${F.rawSpatialRDD.count()}")

        // unpersist RDDs...
        f0_prime.unpersist(false)
        f1_prime.unpersist(false)
        f0.unpersist(false)
        f1.unpersist(false)
      }
    } // rof
    logger.info(s"Number of flocks: ${nF0}")
    val executionTime = "%.2f".format((System.currentTimeMillis() - clockTime) / 1000.0)
    val nReport = nF0
    logger.info(s"PFLOCK|$applicationID|$cores|$executors|$epsilon|$mu|$delta|$executionTime|$nReport")
  }

  def saveFlocks(flocks: RDD[Flock]): Unit = {
    val fw = new java.io.FileWriter("/tmp/flocks.txt", true) ;
    fw.write(flocks.map(f => s"${f.pids.mkString(" ")}\t${f.start}\t${f.end}\n").collect().mkString("")) ;
    fw.close()
  }

  def getFlocksFromGeom(g: Geometry): Flock = {
    val farr   = g.getUserData.toString().split(";")
    val pids   = farr(0).split(" ").map(_.toInt).toList
    val start  = farr(1).toInt
    val end    = farr(2).toInt
    val center = g.getCentroid

    Flock(pids, start, end, center)
  }

  def extractTimestamp(points: PointRDD, timestamp:  Int, sespg: String, tespg: String): PointRDD = {
    val p = new PointRDD(points.rawSpatialRDD.rdd.filter{
      _.getUserData().toString().split("\t").reverse.head.toInt == timestamp
    }.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
    p
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
    var stage = "4a.Partitioning flocks..."
    logStart(stage)

    F.analyze()
    val fullBoundary = F.boundaryEnvelope
    fullBoundary.expandBy(epsilon + precision)

    val boundary = new QuadRectangle(fullBoundary)
    val npartitions = params.ffpartitions()
    val approxCount = F.rawSpatialRDD.count()
    var fraction = 0.0
    var maxLevel = 0
    var maxItemsPerNode = 0
    var samples = F.rawSpatialRDD.rdd
    logger.info(s"approxCount: $approxCount flag: ${approxCount < 100}")
    if(approxCount < 1000){
      fraction = 0.5
      samples = F.rawSpatialRDD.rdd.sample(false, fraction)
      maxLevel = 1
      maxItemsPerNode = approxCount.toInt
  } else {
      val sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(npartitions, approxCount, -1)
      fraction = RDDSampleUtils.getFraction(sampleNumberOfRecords, approxCount)
      samples = F.rawSpatialRDD.rdd.sample(false, fraction)
      maxLevel = npartitions
      maxItemsPerNode = (samples.count() / npartitions).toInt
    }
    if(debug){ logger.info(s"npartitions: $npartitions maxItemsPerNode: $maxItemsPerNode maxLevel: $maxLevel fraction: $fraction")  }
    val quadtreePruneFlocks = new StandardQuadTree[Geometry](boundary, 0, maxItemsPerNode, maxLevel)
    if(debug){ logger.info(s"Disks' size: ${F.rawSpatialRDD.rdd.count()}") }
    if(debug){ logger.info(s"Disks' size of sample: ${samples.count()}") }
    for(sample <- samples.map(_.getEnvelopeInternal).collect()){
      quadtreePruneFlocks.insert(new QuadRectangle(sample), null)
    }
    quadtreePruneFlocks.assignPartitionIds()
    val QTPartitioner = new QuadTreePartitioner(quadtreePruneFlocks)
    val r = (epsilon / 2.0) + precision
    val FCircles = new CircleRDD(F, r)

    FCircles.spatialPartitioning(QTPartitioner)
    //FCircles.spatialPartitioning(GridType.QUADTREE, 512)

    FCircles.spatialPartitionedRDD
    val nFCircles = FCircles.spatialPartitionedRDD.rdd.count()
    val grids = quadtreePruneFlocks.getAllZones.asScala.filter(_.partitionId != null)
      .map(r => r.partitionId -> r.getEnvelope).toMap
    if(debug){ logger.info(s"Disks' partitions: ${grids.size}") }
    logEnd(stage, timer, nFCircles, s"$timestamp")

    timer = System.currentTimeMillis()
    stage = "4b.Finding maximals flocks..."
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
    }.persist(StorageLevel.MEMORY_ONLY_SER)
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
    val debug        = params.ffdebug()
    val info         = params.info()
    cores            = params.cores()
    executors        = params.executors()
    portUI           = params.portui()
    val Mpartitions  = params.mfpartitions()
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
      //.config("spark.cores.max", cores * executors)
      //.config("spark.executor.cores", cores)
      //.master(s"spark://${master}:${port}")
      .appName("PFLock")
      .getOrCreate()
    import spark.implicits._
    appID = spark.sparkContext.applicationId
    startTime = spark.sparkContext.startTime
    //spark.sparkContext.setCheckpointDir(checkpointDir)
    logEnd(stage, timer, 0, "-1")

    // Reading data...
    /*
    timer = System.currentTimeMillis()
    stage = "Data read"
    logStart(stage)
    val points = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true, Dpartitions)
    points.CRSTransform(params.sespg(), params.tespg())
    points.rawSpatialRDD.cache()
    val nPoints = points.rawSpatialRDD.count()
    val timestamps = points.rawSpatialRDD.rdd
      .map(_.getUserData.toString().split("\t").reverse.head.toInt).distinct
      .sortBy(f => f)
      .filter(t => t > mininterval && t < maxinterval)
      .collect().toList
    points.analyze()
    logEnd(stage, timer, nPoints)
    */

    // Quadtree partitioner...
    /*
    timer = clocktime
    stage = "QuadTree partitioner"
    logStart(stage)
    val fullBoundary = points.boundaryEnvelope
    fullBoundary.expandBy(epsilon + precision)
    val samples = points.rawSpatialRDD.rdd
      .sample(false, params.fraction(), 42)
      .map(_.getEnvelopeInternal)
    val boundary = new QuadRectangle(fullBoundary)
    val maxLevel = params.levels()
    val maxItemsPerNode1 = params.entries()
    val quadtree1 = new StandardQuadTree[Geometry](boundary, 0, maxItemsPerNode1, maxLevel)
    if(debug){ logger.info(s"Size of sample: ${samples.count()}") }
    for(sample <- samples.collect()){
      quadtree1.insert(new QuadRectangle(sample), null)
    }
    quadtree1.assignPartitionIds()
    val QTPartitioner = new QuadTreePartitioner(quadtree1)
    logEnd(stage, timer, QTPartitioner.getGrids.size)
    */

    // Running maximal finder...
    timer = System.currentTimeMillis()
    stage = "Flock Finder run"
    logStart(stage)
    var timestamps = (mininterval until maxinterval).toList
    if(params.tsfile() != ""){
      val ts = scala.io.Source.fromFile(params.tsfile())
      timestamps =  ts.getLines.map(_.toInt).toList
      ts.close
    }
    if(params.stream()){
      logger.info("Reading data file by file...")
      run(spark, timestamps, params)
    }else {
      logger.info("Reading full dataset...")
      val data = spark.read.option("header", false).option("delimiter", "\t")
        .csv(input).rdd.map{ row =>
          val pid = row.getString(0).toInt
          val x = row.getString(1).toDouble
          val y = row.getString(2).toDouble
          val t = row.getString(3).toInt
          ST_Point(pid, x, y, t)
        }
      timestamps = data.map(_.t).distinct().collect().toList.sorted
      run2(spark, timestamps, data, params)
    }
    logEnd(stage, timer, 0, tag)

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
      f.write(InfoTracker.getTasksInfoByDuration(25))
      f.close()
    }
    spark.close()
    logEnd(stage, timer, 0, tag)    
  }
}
