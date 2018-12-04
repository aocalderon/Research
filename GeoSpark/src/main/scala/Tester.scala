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
import scala.collection.JavaConverters._
import SPMF.{AlgoLCM2, Transactions}
import SPMF.ScalaLCM.{IterativeLCMmax, Transaction}

object Tester{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val precision: Double = 0.001

  def main(args: Array[String]) = {
    val params = new TesterConf(args)
    val debug: Boolean  = params.debug()
    val master: String  = params.master()
    val epsilon: Double = params.epsilon()
    val mu: Int         = params.mu()
    val input: String   = params.input()
    val offset: Int     = params.offset()
    val partitions: Int = params.partitions()

    // Starting session...
    var timer = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("GeoSparkTester").setMaster(master)
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    val sc = new SparkContext(conf)
    log("Session started", timer)

    // Reading data...
    timer = System.currentTimeMillis()
    val points = new PointRDD(sc, input, offset, FileDataSplitter.TSV, true, partitions)
    points.CRSTransform("epsg:3068", "epsg:3068")
    val nPoints = points.rawSpatialRDD.count()
    log("Data read", timer, nPoints)

    // Finding pairs...
    timer = System.currentTimeMillis()
    val pointsBuffer = new CircleRDD(points, epsilon + precision)
    points.analyze()
    pointsBuffer.analyze()
    pointsBuffer.spatialPartitioning(GridType.QUADTREE)
    points.spatialPartitioning(pointsBuffer.getPartitioner)
    points.buildIndex(IndexType.QUADTREE, true)
    points.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    pointsBuffer.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

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
    log("Pairs found", timer, nPairs)

    // Finding centers...
    timer = System.currentTimeMillis()
    val r2: Double = math.pow(epsilon / 2.0, 2)
    val centers = pairs.map{ p =>
        val p1 = p._1._2
        val p2 = p._2._2
        calculateCenterCoordinates(p1, p2, r2)
      }
    val nCenters = centers.count()
    log("Centers found", timer, nCenters)

    // Finding disks...
    timer = System.currentTimeMillis()
    val r = epsilon / 2.0
    val centersRDD = new PointRDD(centers.map(_._1).union(centers.map(_._2)).toJavaRDD(), StorageLevel.MEMORY_ONLY, "epsg:3068", "epsg:3068")
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
    val nDisks = 0//disks.count()
    log("Disks found", timer, nDisks)

    // Indexing disks...
    timer = System.currentTimeMillis()
    val disksRDD = new PointRDD(disks
      .map(a => (a._2.sorted, a._1))
      .reduceByKey((a,b) => a)
      .map{ d =>
        d._2.setUserData(d._1.mkString(" "))
        d._2
      }.toJavaRDD(), StorageLevel.MEMORY_ONLY, "epsg:3068", "epsg:3068")
    disksRDD.analyze()
    val nDisksRDD = disksRDD.rawSpatialRDD.count()
    disksRDD.spatialPartitioning(GridType.QUADTREE, params.dpp())
    disksRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    log("Disks indexed", timer, nDisksRDD)
    
    // Getting expansions...
    timer = System.currentTimeMillis()
    val expansions = disksRDD.getPartitioner.getGrids.asScala.map{e => e.expandBy(epsilon + precision); e}.zipWithIndex
    val rtree = new STRtree()
    expansions.foreach{e => rtree.insert(e._1, e)}
    val expansionsRDD = disksRDD.spatialPartitionedRDD.rdd.flatMap{ disk =>
      rtree.query(disk.getEnvelopeInternal).asScala.map{e =>
        val (expansion, expansion_id) = castEnvelopeInt(e)
        val notInExpansion = isNotInExpansionArea(disk, expansion, epsilon + precision)
        val userData = s"${disk.getUserData.toString()}\t$notInExpansion"
        disk.setUserData(userData)

        (expansion_id, disk)
      }.toList
    }.partitionBy(new ExpansionPartitioner(expansions.size)).map(_._2)
    val nExpansionsRDD = expansionsRDD.count()
    log("Expansions gotten", timer, nExpansionsRDD)
    
    ///////////////////////////////////////////////////////////////////////////////////
    saveLines(expansionsRDD.mapPartitionsWithIndex{ (i, data) =>
        data.map(d => s"${d.toText()};${d.getUserData.toString()};${i}\n")
    }, "/tmp/disksExpanded.wkt")
    ///////////////////////////////////////////////////////////////////////////////////

    // Finding maximal disks...
    timer = System.currentTimeMillis()
    val maximals = expansionsRDD.mapPartitions{ disks =>
      val transactions = disks.map{ d =>
        d.getUserData.toString().split("\t").head.split(" ").map(new Integer(_)).toList.asJava
      }.toList.asJava
      val LCM = new AlgoLCM2
      val data = new Transactions(transactions)
      LCM.run(data).asScala.map(m => m.asScala.toList.map(_.toInt).sorted).toIterator
    }
    val nMaximals = maximals.count()
    log("Maximal disks found", timer, nMaximals)

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

    // Closing session...
    timer = System.currentTimeMillis()
    sc.stop()
    log("Session closed", timer)
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

  def log(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
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
