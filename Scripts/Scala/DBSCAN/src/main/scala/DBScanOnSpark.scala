import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType}
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PointRDD, CircleRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Coordinate, Point}
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import dbscan._

object DBScanOnSpark {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val precision: Double = 0.0001
  private var startTime: Long = System.currentTimeMillis()

  case class ST_Point(tid: Int, x: Double, y: Double, t: Int) extends Ordered[ST_Point]{
    def distance(other: ST_Point): Double = {
      math.sqrt(math.pow(this.x - other.x, 2) + math.pow(this.y - other.y, 2))
    }

    def getJTSPoint: Point = {
      val point = geofactory.createPoint(new Coordinate(this.x, this.y))
      point.setUserData(s"${this.tid}\t${this.t}")
      point
    }

    override def compare(that: ST_Point): Int = {
      if (x == that.x) y compare that.y
      else x compare that.x
    }

    def canEqual(a: Any) = a.isInstanceOf[ST_Point]

    override def equals(that: Any): Boolean =
      that match {
        case that: ST_Point => {
          that.canEqual(this) && this.x == that.x && this.y == that.y
        }
        case _ => false
      }

    override def toString: String = s"$tid\t$x\t$y\t$t\n"
  }

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def round3(n: Double): Double = { math.round( n * 1000) / 1000 }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("DBSCAN|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new DBScanOnSparkConf(args)
    val debug = params.debug()
    val input = params.input()
    val output = params.output()
    val cores = params.cores()
    val executors = params.executors()
    val master = params.local() match {
      case true  => s"local[${cores}]"
      case false => s"spark://${params.host()}:${params.port()}"
    }

    var timer = clocktime
    var stage = "Session start"
    log(stage, timer, 0, "START")
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * cores * executors)
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.cores.max", cores * executors)
      .config("spark.executor.cores", cores)
      .config("spark.kryoserializer.buffer.max.mb", "1024")
      .master(master)
      .appName("LATrajCleaner")
      .getOrCreate()
    import spark.implicits._
    startTime = spark.sparkContext.startTime
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    val minPts = params.minpts()
    val epsilon = params.epsilon()
    val points = spark.read
      .option("header", false)
      .option("delimiter", "\t")
      .csv(input).rdd.map{ row =>
        val tid = row.getString(0).toInt
        val x = row.getString(1).toDouble
        val y = row.getString(2).toDouble
        val t = row.getString(3).toInt
        val coord = new Coordinate(x, y)
        val p = geofactory.createPoint(coord)
        p.setUserData(s"$tid\t$t")
        p
      }.cache()
    val pointsRDD = new SpatialRDD[Point]()
    pointsRDD.setRawSpatialRDD(points)
    pointsRDD.analyze()
    val nPoints = points.count()
    log(stage, timer, nPoints, "END")
    
    timer = clocktime
    stage = "Partitioning"
    log(stage, timer, 0, "START")
    val circlesRDD = new CircleRDD(pointsRDD, params.epsilon())
    circlesRDD.analyze()
    circlesRDD.spatialPartitioning(GridType.EQUALGRID, params.partitions())
    pointsRDD.spatialPartitioning(circlesRDD.getPartitioner)
    val considerBoundaryIntersection = true
    val usingIndex = true
    val pairs = JoinQuery.DistanceJoinQueryFlat(pointsRDD, circlesRDD, usingIndex, considerBoundaryIntersection)
      .rdd
      .flatMap { pair =>
        val p1 = pair._1.asInstanceOf[Point]
        val p2 = pair._2
        List(p1, p2)
      }
      .distinct()
      .cache()
    val nPairs = pairs.count()
    log(stage, timer, nPairs, "END")
    
    timer = clocktime
    stage = "DBScan run"
    log(stage, timer, 0, "START")
    val algo = new AlgoDBSCAN()
    val data = pairs.map{ p =>
      val arr = p.getUserData().toString().split("\t")
      val tid = arr(0).toInt
      val t = arr(1).toInt
      val x = p.getX
      val y = p.getY
      val da = new DoubleArray(Array(x, y))
      da.setTid(tid)
      da.setT(t)
      da
    }.collect().toList.asJava
    val clusters = algo.run(data, minPts, epsilon).asScala.toList
    val nClusters = clusters.size
    log(stage, timer, 0, "END")

    if(debug){
      clusters.zipWithIndex.map{ case (cluster, i) =>
        val xs = cluster.getVectors.asScala.map(_.get(0)).reduce(_ + _)
        val ys = cluster.getVectors.asScala.map(_.get(1)).reduce(_ + _)
        val count = cluster.getVectors.size().toDouble

        s"Cluster's centroid ${i} (${xs/count}, ${ys/count})"
      }.foreach(println)
      algo.printStatistics()
    }

    val r  = params.epsilon()/2.0
    val r2 = math.pow(r, 2)
    def computeCenters(p1: ST_Point, p2: ST_Point): (Point, Point) = {
      var h = geofactory.createPoint(new Coordinate(-1.0,-1.0))
      var k = geofactory.createPoint(new Coordinate(-1.0,-1.0))
      val X: Double = p1.x - p2.x
      val Y: Double = p1.y - p2.y
      val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
      if (D2 != 0.0){
        val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
        val h1: Double = ((X + Y * root) / 2) + p2.x 
        val k1: Double = ((Y - X * root) / 2) + p2.y 
        val h2: Double = ((X - Y * root) / 2) + p2.x 
        val k2: Double = ((Y + X * root) / 2) + p2.y 
        h = geofactory.createPoint(new Coordinate(h1,k1))
        k = geofactory.createPoint(new Coordinate(h2,k2))
      }
      (h, k)
    }

    clusters.map{ cluster =>
      val points = cluster.getVectors().asScala.map{ v =>
        ST_Point(v.getTid, v.get(0), v.get(1), v.getT)
      }
      val pairs = points.cross(points)
        .filter( p => p._1 < p._2)
        .map(p => (p, p._1.distance(p._2)))
        .filter(p => p._2 <= params.epsilon())
        .map{ p =>
          val p1 = p._1._1
          val p2 = p._1._2
          computeCenters(p1, p2)
        }.toList
      val centers = pairs.map(_._1).union(pairs.map(_._2))
      val rtree = new com.vividsolutions.jts.index.strtree.STRtree()
      for(point <- points.map(_.getJTSPoint)){
        rtree.insert(point.getEnvelopeInternal, point)
      }
      val disks = centers.map{ center =>
        rtree.query(center.buffer(r).getEnvelopeInternal).asScala
          .map(_.asInstanceOf[Point])
          .filter(point => center.distance(point) <= r)
          .map(point => (center, point)).groupBy(_._1).toList.map{ p =>
            val key = p._1 // the center
            val value = p._2.toList.map(_._2) // list of points
            (key, value)
          }
        .filter(_._2.size > params.mu())
      }

      // Prune duplicates and redundant disks...
    }

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class DBScanOnSparkConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (default = Some("/tmp/test.tsv"))
  val output:     ScallopOption[String]  = opt[String]  (default = Some("/tmp/output"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("KDBTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val n:          ScallopOption[Double]  = opt[Double]  (default = Some(100.0))
  val m:          ScallopOption[Int]     = opt[Int]     (default = Some(200))
  val minpts:     ScallopOption[Int]     = opt[Int]     (default = Some(5))
  val epsilon:    ScallopOption[Double]  = opt[Double]  (default = Some(5.0))
  val mu:         ScallopOption[Int]     = opt[Int]     (default = Some(5))

  verify()
}
