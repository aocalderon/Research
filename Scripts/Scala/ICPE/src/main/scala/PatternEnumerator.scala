import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PointRDD, CircleRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Coordinate, Point}
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object PatternEnumerator {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val precision: Double = 0.0001
  private var startTime: Long = System.currentTimeMillis()
  private var applicationID: String = "app-00000000000000-0000"

  case class Pids(t: Int, pids: List[Int])

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def round3(n: Double): Double = { math.round( n * 1000) / 1000 }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("ICPE|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new PatternEnumeratorConf(args)
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
      //.config("spark.cores.max", cores * executors)
      //.config("spark.executor.cores", cores)
      //.master(master)
      .appName("PatternEnumerator")
      .getOrCreate()
    import spark.implicits._
    startTime = spark.sparkContext.startTime
    applicationID = spark.sparkContext.applicationId
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    val epsilon = params.epsilon()
    val mu = params.mu()
    val disks = spark.read
      .option("header", false)
      .option("delimiter", "\t")
      .csv(input).map{ row =>
        val t = row.getString(0).toInt
        val x = row.getString(1).toDouble
        val y = row.getString(2).toDouble
        val pids = row.getString(3).split(" ").map(_.toInt).toSet

        (t, Disk(x, y, pids))
      }.cache()
    val nDisks = disks.count()
    log(stage, timer, nDisks, "END")

    disks.map(p => s"${p._1}\t${p._2.toString()}").show(truncate=false)

    val partitions = disks.flatMap{ disk =>
      val t = disk._1
      val pids = disk._2.pids.toList.sorted
      pids.map{ pid =>
        (pid, Pids(t, pids.filter(_ > pid)))
      }
    }.filter(!_._2.pids.isEmpty)

    partitions.toDF().show(100, truncate=false)
    val data = partitions.repartition($"_1")
    data.rdd.mapPartitionsWithIndex{ case (i, pids) => 
      pids.map(p => s"${i}->${p._1}: ${p._2}") 
    }.foreach(println)
  
    if(debug){
      logger.info("PatternEnumerator|%s|%5.1f|%2d|%6.2f|%6d".format(applicationID, epsilon, mu, ((clocktime - timer) / 1000.0)))
    }

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class PatternEnumeratorConf(args: Seq[String]) extends ScallopConf(args) {
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
  val minpts:     ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val epsilon:    ScallopOption[Double]  = opt[Double]  (default = Some(5.0))
  val mu:         ScallopOption[Int]     = opt[Int]     (default = Some(5))

  verify()
}
