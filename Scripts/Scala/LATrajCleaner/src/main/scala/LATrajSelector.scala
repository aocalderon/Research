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

  case class ST_Point(pid: Long, x: Double, y: Double, t: Int)

  case class Traj(tid: Long, points: List[String]){
    var t: Int = -1
    var p: List[ST_Point] = List.empty[ST_Point]

    def setInitialTime(t: Int): Unit = {
      val size = points.size
      val newts = t until (t + size)
      this.p = newts.zip(points).map(p => ST_Point(tid, p._2.split(" ")(0).toDouble, p._2.split(" ")(1).toDouble, p._1)).toList
      this.t = t
    }

    def toWKT: String = {
      s"LINESTRING ( ${points.mkString(",")} )\t${tid}"
    }
  }

  def round3(n: Double): Double = { math.round( n * 1000) / 1000 }

  def savePoints(points: List[ST_Point], filename: String): Unit = {
    val f = new PrintWriter(filename)
    f.write(points.map{ p =>
      s"${p.pid}\t${p.x}\t${p.y}\t${p.t}\n"
    }.mkString(""))
    f.close()
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("LATC|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new LATrajSelectorConf(args)
    val input = params.input()
    val output = params.output()
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
    val points0 = spark.read.option("header", "false").option("delimiter", "\t").csv(input).rdd
      .map{ p =>
        val pid = p.getString(0).toInt
        val x   = p.getString(1).toDouble
        val y   = p.getString(2).toDouble
        val t   = p.getString(3).toInt
        val point = geofactory.createPoint(new Coordinate(x, y))
        point.setUserData(s"$pid\t$t")
        point
      }.cache()
    //val pointsRDD = new SpatialRDD[Point]()
    //pointsRDD.setRawSpatialRDD(points0)
    //pointsRDD.analyze()
    //pointsRDD.spatialPartitioning(GridType.KDBTREE, partitions)
    //pointsRDD.spatialPartitionedRDD.cache()
    //var nPoints = pointsRDD.spatialPartitionedRDD.count()
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Trajectory detection"
    log(stage, timer, 0, "START")
    //val points = pointsRDD.spatialPartitionedRDD.rdd.map{ point =>
    val points = points0.map{ point =>
      val userData = point.getUserData.toString().split("\t")
      val pid = userData(0).toInt
      val t = userData(1).toInt
      val x = point.getX
      val y = point.getY
      ST_Point(pid, x, y, t)
    }.toDS().cache()
    val trajs = points.map(p => (p.pid, p.t, s"${p.x} ${p.y}")).toDF("pid", "t", "point")
      .orderBy($"pid", $"t")
      .groupBy($"pid")
      .agg(collect_list($"point").alias("points"))
      .map{ p =>
        val tid = p.getLong(0)
        val points = p.get(1).asInstanceOf[WrappedArray[String]]
        Traj(tid, points.toList)
      }.cache()
    val nTrajs = trajs.count()
    log(stage, timer, nTrajs, "END")

    def getPointsByTimeInterval(points: Dataset[ST_Point]): List[(Int, Long)] = {
      points.groupBy($"t").count().collect().map(r => (r.getInt(0), r.getLong(1))).toList.sortBy(x => x._1)
    }

    timer=clocktime
    stage="Saving trajs"
    log(stage, timer, 0, "START")
    val data = trajs.map{ traj =>
        s"${traj.tid}\t${traj.points.size}\t${traj.points.mkString(", ")}\n"
      }
      .toDF("traj")
      .select(rand() as "rand", $"traj")
      .orderBy($"rand")
    data.show(false)  
    val f = new java.io.PrintWriter("/tmp/trajs.tsv")
    f.write(data.select($"traj").rdd.map(_.getString(0)).collect().mkString(""))
    f.close()
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Initial sample"
    log(stage, timer, 0, "START")
    val sample1 = trajs.sample(false, params.fraction())
    var reminder = trajs.except(sample1)
    var dataset = sample1.flatMap{ traj =>
      traj.setInitialTime(0)
      traj.p
    }
  
    logger.info(s"Size of original: ${trajs.count()}")
    logger.info(s"Size of sample: ${sample1.count()}")
    logger.info(s"Size of reminders: ${reminder.count()}")
    logger.info(s"Size of dataset: ${dataset.count()}")
    var state = getPointsByTimeInterval(dataset)
    logger.info(s"State: ${state}")

    val limit = sample1.count().toInt
    var n = 1
    var reminderCount = reminder.count()
    while(false){
      val left = limit - state.head._2
      var fraction = (left * 1.0) / reminderCount
      if(fraction > 1.0) { fraction = 1.0 }
      val newTrajs = reminder.sample(false, fraction)
      val newPoints = newTrajs.flatMap{ traj =>
        traj.setInitialTime(n)
        traj.p
      }
      val newState = getPointsByTimeInterval(newPoints)
      state = (state ++ newState).groupBy(s => s._1).mapValues(f => f.map(_._2).reduce(_ + _)).toList.sortBy(_._1).filter(_._1 > n)

      reminder = reminder.except(newTrajs).persist(StorageLevel.MEMORY_AND_DISK)
      reminderCount = reminder.count()
     
      logger.info(s"State: ${state}")

      logger.info(s"In time interval $n")
      logger.info(s"Size of original: ${nTrajs}")
      logger.info(s"New trajs: ${left}")
      logger.info(s"Size of reminders: ${reminderCount}")
      n = n + 1
    }
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
  val fraction:   ScallopOption[Double]  = opt[Double]  (default = Some(0.1))

  verify()
}
