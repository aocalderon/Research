import com.vividsolutions.jts.geom.{Point, Geometry, GeometryFactory, Coordinate}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import SPMF.{AlgoLCM2, Transactions}
import SPMF.ScalaLCM.{IterativeLCMmax, Transaction}

object Tester{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory(null);
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
    val nDisks = disks.count()
    log("Disks found", timer, nDisks)

    // Finding maximal disks...
    timer = System.currentTimeMillis()
    val diskRDD = new PointRDD(disks
      .map(a => (a._2.sorted, a._1))
      .reduceByKey((a,b) => a)
      .map{ d =>
        d._2.setUserData(d._1.mkString(" "))
        d._2
      }.toJavaRDD(), StorageLevel.MEMORY_ONLY, "epsg:3068", "epsg:3068")
    diskRDD.analyze()
    val dPartitions = math.ceil(nDisks / params.dpp()).toInt
    diskRDD.spatialPartitioning(GridType.QUADTREE, dPartitions)
    val nDiskRDD = diskRDD.spatialPartitionedRDD.count()
    log("Disks indexed", timer, nDiskRDD)

    timer = System.currentTimeMillis()
    val maximals = diskRDD.spatialPartitionedRDD.rdd.mapPartitions{ disks =>
      val transactions = disks.map{_.getUserData().toString().split("\t").head.split(" ").map(new Integer(_)).toList.asJava}.toList.asJava
      val LCM = new AlgoLCM2
      val data = new Transactions(transactions)
      LCM.run(data).asScala.map(m => m.asScala.toList.map(_.toInt).sorted).toIterator
    }
    val nMaximals = maximals.count()
    log("Maximal disks found", timer, nMaximals)

    timer= System.currentTimeMillis()
    val global = maximals.map(m => m.map(new Integer(_)).asJava).collect().toList.asJava
    val T = new Transactions(global)
    val lcm = new AlgoLCM2
    val finalDisks = lcm.run(T)
    val nFinalDisks = finalDisks.size()
    log("Final disks found", timer, nFinalDisks)

    if(debug){
      val p = pairs.map(c => s"LINESTRING(${c._1._2.getX()} ${c._1._2.getY()}, ${c._2._2.getX()} ${c._2._2.getY()})\n")
      saveLines(p, "/tmp/pairs.wkt")
      val grids = diskRDD.getPartitioner.getGrids().asScala.toList.map{ e =>
        s"POLYGON((${e.getMinX} ${e.getMinY},${e.getMinX} ${e.getMaxY},${e.getMaxX} ${e.getMaxY},${e.getMaxX} ${e.getMinY},${e.getMinX} ${e.getMinY}))\n"
      }
      saveList(grids, "/tmp/grids.wkt")
      val expansions = diskRDD.getPartitioner.getGrids().asScala.toList.map{ e =>
        e.expandBy(epsilon + precision)
        s"POLYGON((${e.getMinX} ${e.getMinY},${e.getMinX} ${e.getMaxY},${e.getMaxX} ${e.getMaxY},${e.getMaxX} ${e.getMinY},${e.getMinX} ${e.getMinY}))\n"
      }
      saveList(expansions, "/tmp/expansions.wkt")
      val disks = diskRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (i, data) =>
        data.map(d => s"POINT(${d.getX} ${d.getY}),${d.getUserData.toString()},${i}\n")
      }
      saveLines(disks, "/tmp/disks.wkt")
      val envelopes = diskRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (i,data) =>
        data.map(d => s"${d.getEnvelope().toText()},${i}\n")
      }
      saveLines(envelopes, "/tmp/envelopes.wkt")

      val nPartitions = diskRDD.getPartitioner.numPartitions
      val stats = diskRDD.spatialPartitionedRDD.rdd.mapPartitions{ p =>
        List(p.length).toIterator
      }
      logger.info(s"Number of partitions in disks: $nPartitions")
      logger.info(s"Min number of disks per partition: ${stats.min()}")
      logger.info(s"Max number of disks per partition: ${stats.max()}")
      logger.info(s"Min number of disks per partition: ${stats.sum()/stats.count()}")
    }

    // Closing session...
    timer = System.currentTimeMillis()
    sc.stop()
    log("Session closed", timer)
  }

  def calculateCenterCoordinates(p1: Point, p2: Point, r2: Double): (Point, Point) = {
    var h = geometryFactory.createPoint(new Coordinate(-1.0,-1.0))
    var k = geometryFactory.createPoint(new Coordinate(-1.0,-1.0))
    val X: Double = p1.getX - p2.getX
    val Y: Double = p1.getY - p2.getY
    val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
    if (D2 != 0.0){
      val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
      val h1: Double = ((X + Y * root) / 2) + p2.getX
      val k1: Double = ((Y - X * root) / 2) + p2.getY
      val h2: Double = ((X - Y * root) / 2) + p2.getX
      val k2: Double = ((Y + X * root) / 2) + p2.getY
      h = geometryFactory.createPoint(new Coordinate(h1,k1))
      k = geometryFactory.createPoint(new Coordinate(h2,k2))
    }
    (h, k)
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
}
