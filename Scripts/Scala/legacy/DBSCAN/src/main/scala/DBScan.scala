import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType}
import org.datasyslab.geospark.spatialRDD.PointRDD
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Coordinate, Point}
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import dbscan._

object DBScan {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val precision: Double = 0.0001
  private var startTime: Long = System.currentTimeMillis()

  def round3(n: Double): Double = { math.round( n * 1000) / 1000 }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("DBSCAN|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new DBScanConf(args)
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
    val rand = scala.util.Random
    val n = params.n()
	val points = new ListBuffer[DoubleArray]()
	(0 to params.m()).foreach{ j =>
      val x = rand.nextDouble() * n
      val y = rand.nextDouble() * n
	  val vector = Array[Double](x, y)
	  points += new DoubleArray(vector)
	}
    val minPts = params.minpts()
    val epsilon = params.epsilon()
    
    val algo = new AlgoDBSCAN()
    
    val clusters = algo.run(points.toList.asJava, minPts, epsilon)
    clusters.asScala.zipWithIndex.map{ case (cluster, i) =>
      val xs = cluster.getVectors.asScala.map(_.get(0)).reduce(_ + _)
      val ys = cluster.getVectors.asScala.map(_.get(1)).reduce(_ + _)
      val count = cluster.getVectors.size().toDouble

      s"Cluster's centroid ${i} (${xs/count}, ${ys/count})"
    }.foreach(println)
    algo.printStatistics()

    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class DBScanConf(args: Seq[String]) extends ScallopConf(args) {
  val output:     ScallopOption[String]  = opt[String]  (default  = Some("/tmp/output"))
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

  verify()
}
