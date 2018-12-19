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
import SPMF.{AlgoLCM2, Transactions}
import SPMF.ScalaLCM.{IterativeLCMmax, Transaction}

object MF{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val precision: Double = 0.001
  private var tag: String = ""

  def run(spark: SparkSession, points: PointRDD, params: FFConf, info: String = ""): RDD[String] = {
    val debug: Boolean   = params.debug()
    val epsilon: Double  = params.epsilon()
    val mu: Int          = params.mu()
    val dpartitions: Int = params.dpartitions()
    val sespg: String    = params.sespg()
    val tespg: String    = params.tespg()
    tag = s"${params.tag()}|${info}"

    // Indexing points...
    var timer = System.currentTimeMillis()
    val pointsBuffer = new CircleRDD(points, epsilon + precision)
    points.analyze()
    pointsBuffer.analyze()
    pointsBuffer.spatialPartitioning(GridType.QUADTREE)
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
        val c = d._1.getCentroid
        val pid = d._2.getUserData().toString().split("\t").head.trim().toInt
        (c, List(pid))
      }.reduceByKey( (pids1,pids2) => pids1 ++ pids2)
      .filter(d => d._2.length >= mu)
    val nDisks = disks.count()
    log("D.Disks found", timer, nDisks)

    // Indexing disks...
    timer = System.currentTimeMillis()
    val disksRDD = new PointRDD(disks
      .map(a => (a._2.sorted, Array(a._1.getCoordinate)))
      .reduceByKey((a,b) => a ++ b)
      .map{ d =>
        val centroid = geofactory.createMultiPoint(d._2).getEnvelope().getCentroid
        centroid.setUserData(d._1.mkString(" "))
        centroid
      }.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
    disksRDD.analyze()
    val nDisksRDD = disksRDD.rawSpatialRDD.count()
      disksRDD.spatialPartitioning(GridType.QUADTREE, dpartitions)
    disksRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    log("E.Disks indexed", timer, nDisksRDD)
    
    // Getting expansions...
    timer = System.currentTimeMillis()
    val expansions = disksRDD.getPartitioner.getGrids.asScala.map{e => e.expandBy(epsilon + precision); e}.zipWithIndex
    val rtree = new STRtree()
    expansions.foreach{e => rtree.insert(e._1, e._2)}
    val expansionsRDD = disksRDD.spatialPartitionedRDD.rdd.flatMap{ disk =>
      rtree.query(disk.getEnvelopeInternal).asScala.map{expansion_id =>
        (expansion_id, disk)
      }.toList
    }.partitionBy(new ExpansionPartitioner(expansions.size)).persist(StorageLevel.MEMORY_ONLY)
    val nExpansionsRDD = expansionsRDD.count()
    log("F.Expansions gotten", timer, nExpansionsRDD)

    // Finding maximal disks...
    timer = System.currentTimeMillis()
    val candidates = expansionsRDD.map(_._2).mapPartitionsWithIndex{ (expansion_id, disks) =>
      val transactions = disks.map{ d =>
        d.getUserData.toString().split("\t").head.split(" ").map(new Integer(_)).toList.asJava
      }.toList.asJava
      val LCM = new AlgoLCM2
      val data = new Transactions(transactions)
      LCM.run(data).asScala.map{ maximal =>
        (expansion_id, maximal.asScala.toList.map(_.toInt))
      }.toIterator
    }.persist(StorageLevel.MEMORY_ONLY)
    val nCandidates = candidates.count()
    log("G.Maximal disks found", timer, nCandidates)

    // Prunning maximal disks...
    timer = System.currentTimeMillis()
    import spark.implicits._
    val points_positions = points.spatialPartitionedRDD.rdd.map{ point =>
      val point_id = point.getUserData.toString().split("\t").head.trim().toInt
      (point_id, point.getX, point.getY)
    }.toDF("point_id1", "x", "y")
      
    val candidates_points = candidates
        .toDF("expansion_id", "points_ids")
        .withColumn("maximal_id", monotonically_increasing_id())
        .withColumn("point_id2", explode($"points_ids"))
        .select("expansion_id", "maximal_id", "point_id2")
    val expansions_map = expansions.map(e => e._2 -> e._1).toMap
    val maximals = points_positions.join(candidates_points, $"point_id1" === $"point_id2")
      .groupBy($"expansion_id", $"maximal_id")
      .agg(min($"x"), min($"y"), max($"x"), max($"y"), collect_list("point_id1"))
      .map{ m =>
        val expansion = expansions_map(m.getInt(0))
        val x = (m.getDouble(2) + m.getDouble(4)) / 2.0
        val y = (m.getDouble(3) + m.getDouble(5)) / 2.0
        val pids = m.getList[Int](6).asScala.toList.sorted.mkString(" ")
        val notInExpansion = isNotInExpansionArea(
          geofactory.createPoint(new Coordinate(x, y)), expansion, epsilon + precision)
        (pids, x, y, notInExpansion)
      }.filter(_._4)
    .map(m => s"${m._1}\t${m._2}\t{m._3}").rdd

    val nMaximals = maximals.count()
    log("H.Maximal disks prunned", timer, nMaximals)

    if(debug){
      val p = pairs.map(c => s"LINESTRING(${c._1._2.getX()} ${c._1._2.getY()}, ${c._2._2.getX()} ${c._2._2.getY()})\n")
      saveLines(p, "/tmp/pairs.wkt")
      val grids = disksRDD.getPartitioner.getGrids().asScala.toList.map{ e =>
        s"POLYGON((${e.getMinX} ${e.getMinY},${e.getMinX} ${e.getMaxY},${e.getMaxX} ${e.getMaxY},${e.getMaxX} ${e.getMinY},${e.getMinX} ${e.getMinY}))\n"
      }
      saveList(grids, "/tmp/grids.wkt")
      val expansions = disksRDD.getPartitioner.getGrids().asScala.toList.map{ e =>
        e.expandBy(epsilon + precision)
        s"POLYGON((${e.getMinX} ${e.getMinY},${e.getMinX} ${e.getMaxY},${e.getMaxX} ${e.getMaxY},${e.getMaxX} ${e.getMinY},${e.getMinX} ${e.getMinY}))\n"
      }
      saveList(expansions, "/tmp/expansions.wkt")
      val disks = disksRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (i, data) =>
        data.map(d => s"POINT(${d.getX} ${d.getY}),${d.getUserData.toString()},${i}\n")
      }
      saveLines(disks, "/tmp/disks.wkt")
      val envelopes = disksRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (i,data) =>
        data.map(d => s"${d.getEnvelope().toText()},${i}\n")
      }
      saveLines(envelopes, "/tmp/envelopes.wkt")

      val nPartitions = expansionsRDD.getNumPartitions
      val stats = expansionsRDD.mapPartitions{ d =>
        List(d.length).toIterator
      }

      logger.info(s"Number of partitions in disks: $nPartitions")
      logger.info(s"Min number of disks per partition: ${stats.min()}")
      logger.info(s"Max number of disks per partition: ${stats.max()}")
      logger.info(s"Avg number of disks per partition: ${stats.sum()/stats.count()}")
    }

    maximals
  }

  def main(args: Array[String]) = {
    val params = new FFConf(args)
    val master = params.master()
    val input  = params.input()
    val offset = params.offset()
    val ppartitions = params.ppartitions()

    // Starting session...
    var timer = System.currentTimeMillis()
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(master).appName("PFLock").getOrCreate()
    logger.info(s"Session started [${(System.currentTimeMillis - timer) / 1000.0}]")

    // Reading data...
    timer = System.currentTimeMillis()
    val points = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true, ppartitions)
    points.CRSTransform("epsg:3068", "epsg:3068")
    val nPoints = points.rawSpatialRDD.count()
    logger.info(s"Data read [${(System.currentTimeMillis - timer) / 1000.0}] [$nPoints]")

    // Running maximal finder...
    timer = System.currentTimeMillis()
    val maximals = MF.run(spark, points, params)
    val nMaximals = maximals.count()
    logger.info(s"Maximal finder run [${(System.currentTimeMillis() - timer) / 1000.0}] [$nMaximals]")

    // Closing session...
    timer = System.currentTimeMillis()
    spark.stop()
    logger.info(s"Session closed [${(System.currentTimeMillis - timer) / 1000.0}]")
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
    val x = p.getX
    val y = p.getY
    val min_x = e.getMinX
    val min_y = e.getMinY
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
}
