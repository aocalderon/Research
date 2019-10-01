import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import archery._

object GRIndex {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val precision: Double = 0.0001
  private var startTime: Long = System.currentTimeMillis()
  private var applicationID: String = "app-00000000000000-0000"

  case class Key(i: Int, j: Int)

  case class GridObject(key: Key, flag: Boolean, location: ST_Point)

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def round3(n: Double): Double = { math.round( n * 1000) / 1000.0 }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("GRIndex|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def allocateGrid(spark: SparkSession, locations: Dataset[ST_Point], width: Double, epsilon: Double): Dataset[GridObject] = {
    import spark.implicits._
    val grid_objects = locations.flatMap{ location =>
      val i = math.floor(location.x / width).toInt
      val j = math.floor(location.y / width).toInt
      val key = Key(i,j)

      val data_object = List(GridObject(key, false, location))

      val i_start = math.floor((location.x - epsilon) / width).toInt
      val i_end   = math.floor((location.x + epsilon) / width).toInt
      val is = i_start to i_end
      val j_start = math.floor(location.y / width).toInt
      val j_end   = math.floor((location.y + epsilon) / width).toInt
      val js = j_start to j_end 
      val Skeys = is.cross(js).map(c => Key(c._1, c._2)).filterNot(k => k == key).toList

      val query_objects = Skeys.map(key => GridObject(key, true, location))
      
      data_object ++ query_objects
    }.repartition($"key").cache()
    grid_objects
  }

  def queryGrid(spark: SparkSession, gridObjects: Dataset[GridObject], epsilon: Double): Dataset[(ST_Point, ST_Point)] = {
    import spark.implicits._
    val pairs = gridObjects.mapPartitions{ gobjects =>
      val epsilonp = epsilon + precision // Adding a small value to ensure correct results...
      var pairs = new ArrayBuffer[(ST_Point, ST_Point)]()
      var rt: RTree[ST_Point] = RTree()
      gobjects.foreach{ go =>
        val o = go.location
        val bbox: Box = Box(
          (o.x - epsilonp).toFloat, (o.y - epsilonp).toFloat,
          (o.x + epsilonp).toFloat, (o.y + epsilonp).toFloat
        )
        if(!go.flag){
          val query: Seq[Entry[ST_Point]] = rt.search(bbox)
          pairs ++= query.map{ q => (o, q.value) }
          rt = rt.insert(Entry(Point(o.x.toFloat, o.y.toFloat), o))
        } else {
          val query: Seq[Entry[ST_Point]] = rt.search(bbox)
          pairs ++= query.map{ q => (o, q.value) }
        }
      }
      pairs.toIterator
    }.cache
    pairs
  }

  def main(args: Array[String]): Unit = {
    val params = new GRIndexConf(args)
    val debug = params.debug()
    val input = params.input()
    val output = params.output()
    val cores = params.cores()
    val executors = params.executors()
    val epsilon = params.epsilon()
    val width = params.width()
    val master = params.local() match {
      case true  => s"local[${cores}]"
      case false => s"spark://${params.host()}:${params.port()}"
    }

    var timer = clocktime
    var stage = "Session start"
    log(stage, timer, 0, "START")
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * cores * executors)
      .config("spark.scheduler.mode", "FAIR")
      //.config("spark.cores.max", cores * executors)
      //.config("spark.executor.cores", cores)
      //.master(master)
      .appName("ICPE")
      .getOrCreate()
    import spark.implicits._
    startTime = spark.sparkContext.startTime
    applicationID = spark.sparkContext.applicationId
    log(stage, timer, 0, "END")
    
    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    val locations = spark.read
      .option("header", false)
      .option("delimiter", "\t")
      .csv(input).map{ row =>
        val id = row.getString(0).toInt
        val x  = round3(row.getString(1).toDouble)
        val y  = round3(row.getString(2).toDouble)
        val t  = row.getString(3).toInt

        ST_Point(id, x, y, t)
      }.cache()
    val nLocations = locations.count()
    log(stage, timer, nLocations, "END")

    if(debug){
      locations.show(truncate=false)
      logger.info(s"Locations number of partitions: ${locations.rdd.getNumPartitions}")
    }
    
    timer = clocktime
    stage = "Grid allocate"
    log(stage, timer, 0, "START")
    val gridObjects = allocateGrid(spark, locations, width, epsilon)
    val nGridObjects = gridObjects.count
    log(stage, timer, nGridObjects, "END")

    if(debug){
      gridObjects.show(nGridObjects.toInt, truncate=false)
      logger.info(s"Grid objects number of partitions: ${gridObjects.rdd.getNumPartitions}")
    }

    timer = clocktime
    stage = "Grid query"
    log(stage, timer, 0, "START")
    val pairs = queryGrid(spark, gridObjects, epsilon)
    val nPairs = pairs.count()
    log(stage, timer, nPairs, "END")

    if(debug){
      pairs.show(nPairs.toInt, truncate=false)
    }

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class GRIndexConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (default = Some(""))
  val output:     ScallopOption[String]  = opt[String]  (default = Some("/tmp/output"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val epsilon:    ScallopOption[Double]  = opt[Double]  (default = Some(1.5))
  val width:      ScallopOption[Double]  = opt[Double]  (default = Some(3.0))

  verify()
}
