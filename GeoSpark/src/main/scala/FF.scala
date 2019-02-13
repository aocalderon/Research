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
  def runSpatialJoin(spark: SparkSession, pointset: HashMap[Int, PointRDD], params: FFConf): RDD[Flock] = {
    var clockTime = System.currentTimeMillis()
    val debug = params.debug()
    val sespg = params.sespg()
    val tespg = params.tespg()
    val distance = params.distance()
    val dpartitions = params.dpartitions()
    val cores = params.cores()
    val epsilon = params.epsilon()
    val mu = params.mu()
    val delta = params.delta()
    val tag = params.tag()
    var timer = System.currentTimeMillis()
    import spark.implicits._

    // Maximal disks timestamp i...
    var firstRun: Boolean = true
    var report: RDD[Flock] = spark.sparkContext.emptyRDD[Flock]
    var F: PointRDD = new PointRDD(spark.sparkContext.emptyRDD[Point].toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
    var partitioner = F.getPartitioner  
    for(timestamp <- pointset.keys.toList.sorted){
      val T_i = pointset.get(timestamp).get

      // Finding maximal disks...
      //logger.info(s"Starting maximal disks timestamp $timestamp ...")
      timer = System.currentTimeMillis()
      val C = new PointRDD(MF.run(spark, T_i, params, s"$timestamp").map(c => makePoint(c, timestamp)).toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
      val nDisks = C.rawSpatialRDD.count()
      //log(s"Maximal disks timestamp $timestamp", timer, nDisks)
      log("1.Maximal disks found", timer, nDisks)

      if(debug){
          //logger.info("C...")
          //C.rawSpatialRDD.rdd.map(f => f.getUserData.toString()).sortBy(_.toString()).toDF().show(200, false)
      }
      
      if(firstRun){
        F = C
        F.analyze()
        F.spatialPartitioning(GridType.QUADTREE, dpartitions)
        F.buildIndex(IndexType.QUADTREE, true)
        partitioner = F.getPartitioner
        firstRun = false
      } else {
        // Doing join...
        timer = System.currentTimeMillis()
        C.analyze()
        val buffers = new CircleRDD(C, distance)
        buffers.spatialPartitioning(partitioner)
        val R = JoinQuery.DistanceJoinQuery(F, buffers, true, false)
        var flocks0 = R.rdd.flatMap{ case (g: Geometry, h: java.util.HashSet[Point]) =>
          val f0 = getFlocksFromGeom(g)
          h.asScala.map{ point =>
            val f1 = getFlocksFromGeom(point)
            (f0, f1)
          }
        }
        if(debug){
          logger.info(s"FF,Candidates_after_Join,${epsilon},${timestamp},${flocks0.count()}")
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
        log("2.Join done", timer, nFlocks)

        if(debug){
          //logger.info("Join...")
          //flocks.map(f => s"${f.pids.mkString(" ")},${f.start},${f.end}").sortBy(_.toString()).toDF().show(200, false)
          logger.info(s"FF,Less_Than_Mu,${epsilon},${timestamp},${nFlocks}")
        }

        // Reporting flocks...
        timer = System.currentTimeMillis()
        var f0 =  flocks.filter{f => f.end - f.start + 1 >= delta}.persist()
        val f1 =  flocks.filter{f => f.end - f.start + 1 <  delta}.persist()

        val G_prime = new PointRDD(
          flocks.map{ f =>
            f.center.setUserData(f.pids.mkString(" ") ++ s";${f.start};${f.end}")
            f.center
          }.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg
        )
        G_prime.analyze()
        G_prime.spatialPartitioning(GridType.QUADTREE, dpartitions)
        val F_prime = new PointRDD(
          f0.map{ f =>
            f.center.setUserData(f.pids.mkString(" ") ++ s";${f.start};${f.end}")
            f.center
          }.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg
        )
        F_prime.spatialPartitioning(G_prime.getPartitioner)
        log("3.Flocks to report", timer, f0.count())

        // Prunning flocks to report...
        timer = System.currentTimeMillis()
        f0 = pruneFlockByExpansions(F_prime, epsilon, spark).persist()
          .map{ f =>
            val arr = f.split(";")
            val p = arr(0).split(" ").map(_.toInt).toList
            val s = arr(1).toInt
            val e = arr(2).toInt
            val x = arr(3).toDouble
            val y = arr(4).toDouble
            val c = geofactory.createPoint(new Coordinate(x, y))
            Flock(p,s,e,c)
          }.persist(StorageLevel.MEMORY_ONLY)
        report = report.union(f0)
        log("4.Flocks prunned", timer, f0.count())

        if(debug){
          //f0.map(f => s"${f.pids.mkString(" ")};${f.start};${f.end};POINT(${f.center.getX} ${f.center.getY})").toDF("flock").sort("flock").show(f0.count().toInt, false)
          logger.info(s"FF,Flocks_being_reported,${epsilon},${timestamp},${f0.count()}")
        }

        // Indexing candidates...
        timer = System.currentTimeMillis()
        flocks = f0.map(f => Flock(f.pids, f.start + 1, f.end, f.center))
          .union(f1)
          .union(C.rawSpatialRDD.rdd.map(c => getFlocksFromGeom(c)))
          .map(f => (f.pids, f))
          .reduceByKey( (a,b) => if(a.start < b.start) a else b )
          .map(_._2)
          .distinct()
          .persist()

        F = new PointRDD(flocks.map{ f =>
            val info = s"${f.pids.mkString(" ")};${f.start};${f.end}"
            f.center.setUserData(info)
            f.center
          }.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
        F.analyze()
        F.spatialPartitioning(GridType.QUADTREE, dpartitions)
        F.buildIndex(IndexType.QUADTREE, true)
        partitioner = F.getPartitioner
        log("5.Candidates indexed", timer, F.rawSpatialRDD.count())
        if(debug){
          //logger.info("F...")
          //F.rawSpatialRDD.rdd.map(f => f.getUserData.toString()).sortBy(_.toString()).toDF().show(200, false)
          logger.info(s"FF,New_set_of_candidates,${epsilon},${timestamp},${F.rawSpatialRDD.count()}")
        }
      }
    }
    logger.info(s"Number of flocks: ${report.count()}")
    val executionTime = (System.currentTimeMillis() - clockTime) / 1000.0
    logger.info(s"PFLOCK;${tag};${epsilon};${mu};${delta};${executionTime};${report.count()}")
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
    val flocks = runSpatialJoin(spark: SparkSession, pointset: HashMap[Int, PointRDD], params: FFConf)
    logger.info(s"Flock finder run [${(System.currentTimeMillis() - timer) / 1000.0}]")

    if(params.debug()){
      import spark.implicits._
      val f = flocks.map(f => s"${f.start}, ${f.end}, ${f.pids.mkString(" ")}\n").sortBy(_.toString()).collect().mkString("")
      import java.io._
      val pw = new PrintWriter(new File("/tmp/flocks.txt"))
      pw.write(f)
      pw.close
    }

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

  import com.vividsolutions.jts.index.strtree.STRtree
  import org.apache.spark.Partitioner
  import SPMF.{AlgoLCM2, Transactions}

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

  def pruneFlockByExpansions(F: PointRDD, epsilon: Double, spark: SparkSession): RDD[String] = {
    import spark.implicits._
    val debug = false
    val rtree = new STRtree()

    ///
    if(debug) F.rawSpatialRDD.rdd.map(_.getUserData.toString()).toDF("F").filter($"F".contains("297 2122 2224")).orderBy("F").show(F.rawSpatialRDD.rdd.count.toInt, false)

    var timer = System.currentTimeMillis()
    val expansions = F.getPartitioner.getGrids.asScala.map{ e =>
      new Envelope(e.getMinX - epsilon, e.getMaxX + epsilon, e.getMinY - epsilon, e.getMaxY + epsilon)
    }.zipWithIndex
    expansions.foreach{e => rtree.insert(e._1, e._2)}
    val expansions_map = expansions.map(e => e._2 -> e._1).toMap

    val expansionsRDD = F.spatialPartitionedRDD.rdd.flatMap{ disk =>
      rtree.query(disk.getEnvelopeInternal).asScala.map{expansion_id =>
        (expansion_id, disk)
      }.toList
    }.partitionBy(new ExpansionPartitioner(expansions.size)).persist(StorageLevel.MEMORY_ONLY)
    log("a.Making expansions", timer)

    ///
    if(debug) expansionsRDD.map(e => s"${e._2.getUserData.toString()};${e._1}").toDF("E").filter($"E".contains("297 2122 2224")).orderBy("E").show(expansionsRDD.count().toInt, false)

    timer = System.currentTimeMillis()
    val candidates = expansionsRDD.map(_._2).mapPartitionsWithIndex{ (expansion_id, disks) =>
      val transactions = disks.map{ d => d.getUserData.toString().split(";")(0).split(" ").map(new Integer(_)).toList.asJava}.toList.distinct.asJava
      val LCM = new AlgoLCM2
      val data = new Transactions(transactions)
      LCM.run(data).asScala.map{ maximal =>
        (expansion_id, maximal.asScala.toList.map(_.toInt))
      }.toIterator
    }.persist(StorageLevel.MEMORY_ONLY)
    log("b.Finding maximals", timer)

    ///
    if(debug) candidates.map(p => (p._2.sorted.mkString(" "), p._1)).toDF("C", "E").filter($"C".contains("297 2122 2224")).sort("C").show(candidates.count().toInt, false)

    timer = System.currentTimeMillis()
    val f0 = F.rawSpatialRDD.rdd.map(f => getFlocksFromGeom(f))
      .map(f => (f.pids,f.start,f.end,f.center.getX,f.center.getY))
      .toDF("pids","start","end","x","y")
    val f1 = candidates.toDF("eid", "pids")
    val prunned0 = f0.join(f1, "pids")
      .select($"eid", $"pids", $"start", $"end", $"x", $"y")
      .map{ m =>
        val expansion = expansions_map(m.getInt(0))
        val pids = m.getList[Int](1).asScala.toList.mkString(" ")
        val start = m.getInt(2)
        val end = m.getInt(3)
        val x = m.getDouble(4)
        val y = m.getDouble(5)
        val point = geofactory.createPoint(new Coordinate(x, y))
        val notInExpansion = isNotInExpansionArea(point, expansion, epsilon)
        val f = s"${pids};${start};${end};${point.getX};${point.getY}"
        (f, notInExpansion, expansion.toString())
      }.rdd

    val prunned = prunned0.filter(_._2).map(_._1)
      .distinct()
      .persist(StorageLevel.MEMORY_ONLY)
    log("c.Filtering maximal in expansions", timer)

    ///
    if(debug) prunned0.sortBy(p => p._1).toDF("flock", "notInExpansion", "Exp").filter($"flock".contains("297 2122 2224")).show(prunned0.count().toInt, false)

    prunned
  }

  def log(msg: String, timer: Long, n: Long = 0): Unit ={
    if(n == 0)
      logger.info("%-50s|%6.2f".format(msg,(System.currentTimeMillis()-timer)/1000.0))
    else
      logger.info("%-50s|%6.2f|%6d|%s".format(msg,(System.currentTimeMillis()-timer)/1000.0,n,tag))
  }
}
