import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType}
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PointRDD}
import org.geotools.referencing.CRS
import org.geotools.geometry.jts.JTS
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Coordinate, Point, LineString}
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray
import java.io.PrintWriter

object LATrajSelector {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val precision: Double = 0.0001
  private var startTime: Long = System.currentTimeMillis()

  case class Traj(index: Long, tid: Long, lenght: Int, t: Int = -1)

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("LATC|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new LATrajSelectorConf(args)
    val input = params.input()
    val offset = params.offset()
    val cores = params.cores()
    val executors = params.executors()
    val partitions = params.partitions()
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
      .master(master)
      .appName("LATrajCleaner")
      .getOrCreate()
    import spark.implicits._
    startTime = spark.sparkContext.startTime
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    var trajs = spark.read.option("header", "false").option("delimiter", "\t").csv(input)
      .rdd.zipWithUniqueId()
      .map{ traj =>
        val tid = traj._1.getString(0).toLong
        val length = traj._1.getString(1).toInt
        Traj(traj._2, tid, length)
      }.toDS()
    val indices = trajs.map(_.index).collect().sorted
    val indicesCount = indices.size
    val nTrajs = trajs.count()
    log(stage, timer, nTrajs, "END")

    timer = clocktime
    stage = "Initial sample"
    log(stage, timer, 0, "START")
    val m = params.m()
    var start = 0
    var end = m
    val sample = trajs.filter{ traj =>
      val s = indices(start)
      val e = indices(end)
      traj.index >= s & traj.index < e
    }
    val output = new java.io.PrintWriter(params.output())
    val out = s"0\t${sample.count()}"
    output.write(s"${out}\n")
    logger.info(out)
    var trajCount = trajs.count() - (end - start)
    val dataset = sample.map( traj => Traj(traj.tid, traj.lenght, 0))
    def getPointsByTimeInterval(trajs: Dataset[Traj], n: Int): List[(Int, Long)] = {
      val dataset = trajs.flatMap{ traj =>
        (n until (n + traj.lenght)).map(t => (traj.tid, t))
      }.toDF("tid", "t")
      dataset.groupBy($"t").count().collect().map(r => (r.getInt(0), r.getLong(1))).toList.sortBy(x => x._1)
    }
    var state = getPointsByTimeInterval(sample, 0)
    logger.info(s"Current state: ${state.take(5)}")
    logger.info(s"Current state: ${state.reverse.take(5)}")
    var n = 1
    while(trajCount > 0 && n < params.n()){
      val timer2 = clocktime
      val stage = s"Time interval: $n"
      log(stage, timer2, 0, "START")

      val left = m - state(1)._2.toInt
      start = end
      end = end + left
      if(end >= indicesCount) {
        end = indicesCount - 1
      }
      val sample = trajs.filter{ traj =>
        val s = indices(start)
        val e = indices(end)
        traj.index >= s & traj.index < e
      }
      val out = s"$n\t${sample.count()}"
      output.write(s"${out}\n")
      logger.info(out)
      trajCount = trajCount - (end - start)
      logger.info(s"Trajs left: ${trajCount}")
      val newState = getPointsByTimeInterval(sample, n)
      state = (state ++ newState).groupBy(s => s._1).mapValues(f => f.map(_._2).reduce(_ + _)).toList.sortBy(_._1).filter(_._1 >= n)

      logger.info(s"Current state: ${state.take(5)}")
      logger.info(s"Current state: ${state.reverse.take(5)}")
      n = n + 1

      log(stage, timer2, 0, "END")
    }
    output.close()
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class LATrajSelectorConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default  = Some("/tmp/output.tsv"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("KDBTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(256))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val m:          ScallopOption[Int]     = opt[Int]     (default = Some(50000))
  val n:          ScallopOption[Int]     = opt[Int]     (default = Some(200))
  val fraction:   ScallopOption[Double]  = opt[Double]  (default = Some(0.1))

  verify()
}
