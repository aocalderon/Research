import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.math.BigDecimal.RoundingMode
import scala.util.Random

object Tester3 {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val jitter:    Double = 0.5
  var left: Int = 0
  var right: Int = 0
  var n: Long = 0
  var m: Long = 0

  case class ST_Point(id: Long, x: Double, y: Double, t: Int)
  case class Disk(t: Int, ids: String, x: Double, y: Double)
  case class Segment(ids: String, segment: List[Int])

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Tester")
    .getOrCreate()
  import spark.implicits._

  var segments: Dataset[Segment] = spark.sparkContext.emptyRDD[Segment].toDS()
  var nodes: Dataset[Disk] = spark.sparkContext.emptyRDD[Disk].toDS()
  var times: List[Int] = List.empty[Int]
  var time: Int = 0

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("SparkSession has been created...")
    import spark.implicits._

    val data = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .csv("/home/and/Documents/PhD/Research/Validation/trajs.txt")
      .as[String]
    val T = readTrajectories(data).cache()
    n = T.select("id").distinct().count()
    logger.info(s"Trajectories: $n")
    m = T.select("t").distinct().count()
    logger.info(s"Timestamps:   $m")
    logger.info("Reading trajectories...")

    val timestamps = List(4, 0, 2, 3, 1, 8, 6, 7, 5, 10, 9)

    left = 0
    right = 4
    time = timestamps.head
    var timer = System.currentTimeMillis()
    val C_0 = maximals(T).cache()
    nodes = nodes.union(C_0) // if timestamp is first or last in window
    logging("Maximal C0", timer, C_0.count(), "disks")

    time = timestamps(1)
    times = List(4,0).sorted
    timer = System.currentTimeMillis()
    val C_1 = maximals(T).cache()
    nodes = nodes.union(C_1) // if timestamp is first or last in window
    logging("Maximal 1", timer, C_1.count(), "disks")
    timer = System.currentTimeMillis()
    val F_1 = join(C_1)
    nodes = nodes.union(F_1).distinct().cache()
    logging("Join 1", timer, nodes.count(), "nodes")
    timer = System.currentTimeMillis()
    nodes = filter()
    logging("Filter 1", timer, nodes.count(), "nodes")
    show()

    // Timestamp 2
    time = timestamps(2)
    times = List(4,0,2).sorted
    timer = System.currentTimeMillis()
    val C_2 = maximals(T).cache()
    nodes = nodes.union(C_2)
    logging("Maximal 2", timer, C_2.count(), "disks")
    timer = System.currentTimeMillis()
    val F_2 = join(C_2)
    nodes = nodes.union(F_2).distinct().cache()
    logging("Join 2", timer, nodes.count(), "nodes")
    timer = System.currentTimeMillis()
    nodes = filter()
    logging("Filter 2", timer, nodes.count(), "nodes")
    show()

    // Timestamp 3
    time = timestamps(3)
    times = List(4,0,2,3).sorted
    timer = System.currentTimeMillis()
    val C_3 = maximals(T).cache()
    nodes = nodes.union(C_3)
    logging("Maximal 3", timer, C_3.count(), "disks")
    timer = System.currentTimeMillis()
    val F_3 = join(C_3)
    nodes = nodes.union(F_3).distinct().cache()
    logging("Join 3", timer, nodes.count(), "nodes")
    timer = System.currentTimeMillis()
    nodes = filter()
    logging("Filter 3", timer, nodes.count(), "nodes")
    show()

    // Timestamp 4
    time = timestamps(4)
    times = List(4,0,2,3,1).sorted
    timer = System.currentTimeMillis()
      val C_4 = maximals(T).cache()
      nodes = nodes.union(C_4)
    logging("Maximal 4", timer, C_4.count(), "disks")
    timer = System.currentTimeMillis()
      val F_4 = join(C_4)
      nodes = nodes.union(F_4).distinct().cache()
    logging("Join 4", timer, nodes.count(), "nodes")
    timer = System.currentTimeMillis()
      nodes = filter()
    logging("Filter 4", timer, nodes.count(), "nodes")
    show()

    // Timestamp 5
    left = 4
    right = 8
    time = timestamps(5)
    times = List(4,0,2,3,1,8).sorted
    timer = System.currentTimeMillis()
    val C_5 = maximals(T).cache()
    nodes = nodes.union(C_5)
    logging("Maximal 5", timer, C_5.count(), "disks")
    timer = System.currentTimeMillis()
    nodes = nodes.union(join(C_5)).distinct().cache()
    logging("Join 5", timer, nodes.count(), "nodes")
    timer = System.currentTimeMillis()
    nodes = filter().cache()
    logging("Filter 5", timer, nodes.count(), "nodes")
    show()

    /*
    // Timestamp 6
    timer = System.currentTimeMillis()
    val C_6 = maximals(T, timestamps(6)).cache()
    nodes = nodes.union(C_6)
    logging("Maximal 6", timer, C_6.count(), "disks")
    timer = System.currentTimeMillis()
    nodes = nodes.union(join(C_6)).distinct().cache()
    logging("Join 6", timer, nodes.count(), "nodes")
    timer = System.currentTimeMillis()
    nodes = filter(List(4,0,2,3,1,8,6).sorted).cache()
    logging("Filter 6", timer, nodes.count(), "nodes")
    show()
*/
    spark.close
  }

  def maximals(T: Dataset[ST_Point]): Dataset[Disk] = {
    T.filter(_.t == time)
      .map(p => Disk(p.t, getLetter(p.id), p.x, p.y))
      .as[Disk]
  }

  def join(C: Dataset[Disk]): Dataset[Disk] = {
    val i = times.indexOf(time)
    val prev = if(i > 0) times.indexOf(i - 1) else -1
    val next = times.indexOf(i + 1)

    nodes.filter(n => n.t == prev || n.t == next)
      .select("ids")
      .distinct()
      .join(C, "ids")
      .select("t", "ids", "x", "y")
      .as[Disk]
  }

  def filter(): Dataset[Disk] = {
    updateSegments()
    segments.withColumn("t", explode($"segment"))
      .select("t", "ids")
      .join(nodes, Seq("t", "ids"))
      .select("t", "ids", "x",  "y")
      .as[Disk]
  }

  def updateSegments(): Long = {
    segments = nodes.select("ids", "t")
      .groupBy("ids")
      .agg(collect_list("t").alias("times"))
      .map{ t =>
        val ids = t.getString(0)
        val ts = t.getList[Int](1).asScala.toList.sorted

        (ids, checkConsecutiveness(ts, times))
      }
      .toDF("ids", "segments")
      .withColumn("segment", explode($"segments"))
      .select("ids", "segment")
      .map{ s =>
        val ids = s.getString(0)
        val segment = s.getList[Int](1).asScala.toList.sorted

        Segment(ids, segment)
      }
      .filter{ s => s.segment.head == left || s.segment.last == right }
      .as[Segment]
      .cache()

    segments.count()
  }

  def checkConsecutiveness(times: List[Int], timestamps: List[Int]): Array[Array[Int]] = {
    if(times.lengthCompare(2) <= 0) return Array(times.toArray)
    if(times.lengthCompare(timestamps.length) == 0) return Array(times.toArray)
    times.sliding(2)
      .map{ m =>
        if( timestamps.indexOf(m.head) + 1 == timestamps.indexOf(m.last) )
          s"${m.head} ${m.last}"
        else
          s"${m.head};${m.last}"
      }
      .mkString(" ").split(";")
      .map(_.split(" ").map(_.toInt).distinct)
  }

  def readTrajectories(data: Dataset[String]): Dataset[ST_Point] = {
    val n = data.count()
    data.map{ traj =>
        traj.split(" ").map(_.toInt).zip(1 to n.toInt).map(i => i._1 * i._2).filter(_ > 0).map(_ - 1)
      }
      .withColumn("id", monotonically_increasing_id())
      .withColumn("t", explode($"value"))
      .select("id", "t")
      .map { tuple =>
        val id = tuple.getLong(0)
        val t = tuple.getInt(1)
        var s = 1
        if (Random.nextBoolean()) {
          s = 1
        } else {
          s = -1
        }
        val y = id + BigDecimal(Random.nextDouble() * jitter * s).setScale(2, RoundingMode.HALF_UP).toDouble

        ST_Point(id, t, y, t)
      }
  }

  def show(): Unit = {
    showNodes()
    showSegments()
  }

  def showNodes(): Unit = {
    val n = nodes.count()
    nodes.orderBy("ids", "t").show(n.toInt, truncate = false)
    println(s"Number of disks: $n")
  }

  def showSegments(): Unit = {
    val n = segments.count()
    segments.orderBy("ids").show(n.toInt, truncate = false)
    println(s"Number of segments: $n")
  }

  def logging(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    logger.info("%-50s | %6.2fs | %6d %s".format(msg, (System.currentTimeMillis() - timer)/1000.0, n, tag))
  }


  def getLetter(id: Long): String = {
    id match {
      case 0 => "A"
      case 1 => "B"
      case 2 => "C"
      case 3 => "D"
      case 4 => "E"
      case 5 => "F"
      case 6 => "G"
      case 7 => "H"
      case 8 => "I"
      case 9 => "J"
      case 10 => "K"
      case 11 => "L"
    }
  }
}
