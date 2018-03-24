import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.math.BigDecimal.RoundingMode
import scala.util.Random

object Tester2 {
  private val log: Logger = LoggerFactory.getLogger("myLogger")
  private val AxisColor: String = "black"
  private val LineStyle: String = "dashed"
  private val DotSize:   Double = 0.2
  private val AxisWidth: Double = 0.05
  private val jitter:    Double = 0.5
  var axis: String   = ""
  var points: String = ""
  var lines: String  = ""
  var left: Int = 0
  var right: Int = 0
  var n: Long = 0
  var m: Long = 0

  case class ST_Point(id: Long, x: Double, y: Double, t: Int)

  case class Link(t1: Int, ids1: String, t2: Int, ids2: String)

  case class Disk(t: Int, ids: String, x: Double, y: Double)

  case class Flock(start: Int, end: Int, ids: String, x: Double, y: Double)

  case class Segment(ids: String, segment: List[Int])

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Tester")
    .getOrCreate()
  import spark.implicits._
  var segments: Dataset[Segment] = spark.sparkContext.emptyRDD[Segment].toDS()

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    log.info("SparkSession has been created...")
    import spark.implicits._

    val file = spark.read.option("header", "false").option("delimiter", ",").csv("/home/and/Documents/PhD/Research/Validation/trajs.txt")
    val nFile = file.collect().length
    val T = file.as[String]
      .map(traj => traj.split(" ").map(_.toInt).zip(1 to nFile).map(i => i._1 * i._2).filter(_ > 0).map(_ - 1))
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
      }.cache()
    n = T.select("id").distinct().count()
    log.info(s"Trajectories: $n")
    m = T.select("t").distinct().count()
    log.info(s"Timestamps:   $m")
    log.info("Reading trajectories...")
    val LaTeX = new java.io.PrintWriter("/home/and/Documents/PhD/Research/Validation/LaTeX/graph.tex")

    val timestamps = List(4, 0, 2, 3, 1, 8, 6, 7, 5, 10, 9)

    var nodes = spark.sparkContext.emptyRDD[Disk].toDS()
    //var links = spark.sparkContext.emptyRDD[Link].toDS()

    left = 0
    right = 4
    val C_0 = maximals(T, timestamps.head)
    nodes = nodes.union(C_0) // if timestamp is first or last in window
    val C_1 = maximals(T, timestamps(1))
    nodes = nodes.union(C_1) // if timestamp is first or last in window
    setAxis(nodes)
    setPoints(nodes)
    LaTeX.write(parser("Maximal disks"))

    nodes = nodes.union(join(nodes, C_1)).distinct()
    setLines(nodes, List(4,0).sorted)
    LaTeX.write(parser("Join"))

    nodes = filter(nodes, List(4,0).sorted)
    setPoints(nodes)
    setLines(nodes, List(4,0).sorted)
    LaTeX.write(parser("Filter"))

    // Timestamp 2
    val C_2 = maximals(T, timestamps(2))
    nodes = nodes.union(C_2)
    setAxis(nodes)
    setPoints(nodes)
    LaTeX.write(parser("Maximal disks"))

    nodes = nodes.union(join(nodes, C_2)).distinct()
    setLines(nodes, List(4,0,2).sorted)
    LaTeX.write(parser("Join"))

    nodes = filter(nodes, List(4,0,2).sorted)
    setPoints(nodes)
    setLines(nodes, List(4,0,2).sorted)
    LaTeX.write(parser("Filter"))

    // Timestamp 3
    val C_3 = maximals(T, timestamps(3))
    nodes = nodes.union(C_3)
    setAxis(nodes)
    setPoints(nodes)
    LaTeX.write(parser("Maximal disks"))

    nodes = nodes.union(join(nodes, C_3)).distinct()
    setLines(nodes, List(4,0,2,3).sorted)
    LaTeX.write(parser("Join"))

    nodes = filter(nodes, List(4,0,2,3).sorted)
    setPoints(nodes)
    setLines(nodes, List(4,0,2,3).sorted)
    LaTeX.write(parser("Filter"))

    // Timestamp 4
    val C_4 = maximals(T, timestamps(4))
    nodes = nodes.union(C_4)
    setAxis(nodes)
    setPoints(nodes)
    LaTeX.write(parser("Maximal disks"))

    nodes = nodes.union(join(nodes, C_4)).distinct()
    setLines(nodes, List(4,0,2,3,1).sorted)
    LaTeX.write(parser("Join"))

    nodes = filter(nodes, List(4,0,2,3,1).sorted)
    setPoints(nodes)
    setLines(nodes, List(4,0,2,3,1).sorted)
    LaTeX.write(parser("Filter"))

    left = 4
    right = 8
    // Timestamp 4
    val C_5 = maximals(T, timestamps(5))
    nodes = nodes.union(C_5)
    setAxis(nodes)
    setPoints(nodes)
    LaTeX.write(parser("Maximal disks"))

    nodes = nodes.union(join(nodes, C_5)).distinct()
    setLines(nodes, List(4,0,2,3,1,8).sorted)
    LaTeX.write(parser("Join"))

    nodes = filter(nodes, List(4,0,2,3,1,8).sorted)
    setPoints(nodes)
    setLines(nodes, List(4,0,2,3,1,8).sorted)
    LaTeX.write(parser("Filter"))

    LaTeX.close()
    spark.close
  }

  def maximals(T: Dataset[ST_Point], t: Int): Dataset[Disk] = {
    T.filter(_.t == t).map(p => Disk(p.t, getLetter(p.id), p.x, p.y)).as[Disk]
  }

  def join(nodes: Dataset[Disk], C: Dataset[Disk]): Dataset[Disk] = {
    nodes.select("ids").distinct().join(C, "ids").select("t", "ids", "x", "y").as[Disk]
  }

  def filter(nodes: Dataset[Disk], timestamps: List[Int]): Dataset[Disk] = {
    nodes.select("ids", "t")
      .groupBy("ids")
      .agg(collect_list("t").alias("times"))
      .map{ t =>
        val ids = t.getString(0)
        val times = t.getList[Int](1).asScala.toList.sorted

        (ids, checkConsecutiveness(times, timestamps))
      }
      .toDF("ids", "segments")
      .withColumn("segment", explode($"segments"))
      .select("ids", "segment")
      .filter{ s =>
        val segment = s.getList[Int](1).asScala.toList

        segment.head == left || segment.last == right
      }
      .withColumn("t", explode($"segment"))
      .select("t", "ids")
      .join(nodes, Seq("t", "ids"))
      .select("t", "ids", "x",  "y")
      .as[Disk]
  }

  def checkConsecutiveness(times: List[Int], timestamps: List[Int]): Array[Array[Int]] = {
    if(times.lengthCompare(1) == 0) return Array(times.toArray)
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

  def parser(phase: String): String = {
    log.info(s"Parsing $phase...")
    val header  = parserHeader(phase)
    val content = s"$axis\n\t\t$points\n\t\t$lines"
    val footer  = parserFooter()

    s"$header$content$footer"
  }

  def parserHeader(phase: String): String = {
    val section = s"\n\\section*{$phase}\n\t\\vspace{1cm}\n"
    val scale = s"\t\\psscalebox{1.0 1.0} {\n"
    val begin = s"\t\\begin{pspicture}(0,0.0)($m,$n)\n\t\t"

    s"$section$scale$begin"
  }

  def parserFooter(): String = {
    "\n\t\\end{pspicture}\n}\n\\clearpage\n"
  }

  def setAxis(nodes: Dataset[Disk]): Unit = {
    axis = nodes.select("t").distinct().orderBy("t")
      .map { t =>
        val x = t.getInt(0)
        val psline = s"\\psline[linecolor=$AxisColor, linewidth=$AxisWidth]"
        val uput = "\\uput[90](%d, %d){$t_{%d}$}".format(x, n, x)

        s"$psline ($x,-1)($x,$n) $uput"
      }
      .collect()
      .mkString("\n\t\t")
  }

  def setPoints(nodes: Dataset[Disk]): Unit = {
    points = nodes.map { t =>
        val name = s"${t.ids} ${t.t}"
        val coords = s"${t.x},${t.y}"
        s"\\dotnode[dotstyle=*,linecolor=${getColor(t.ids)},dotsize=$DotSize] ($coords) {$name}"
      }
      .collect()
      .mkString("\n\t\t")
  }

  def setLines(nodes: Dataset[Disk], timestamps: List[Int]): Unit = {
    val t0 = nodes.select("ids","t")
      .orderBy("ids", "t")
      .groupBy("ids").agg(collect_list($"t"))
      .map{ t =>
        val ids = t.getString(0)
        val times = t.getList[Int](1).asScala.toList.sorted

        (ids, checkConsecutiveness(times, timestamps))
      }
      .toDF("ids", "segments")
      .withColumn("segment", explode($"segments"))
      .select("ids", "segment")
    t0.orderBy("ids").show(200, truncate = false)

    val t1 = t0.filter(s => s.getList[Int](1).asScala.lengthCompare(1) > 0).flatMap { l =>
        val ids = l.getString(0)
        val segments = l.getList[Int](1).asScala.toList.sorted.sliding(2)
        segments.map{ s =>
          val node1 = s"$ids ${s.head}"
          val node2 = s"$ids ${s.last}"
          s"\\ncline[linecolor=${getColor(ids)},linestyle=$LineStyle]{$node1}{$node2}"
        }
      }
    t1.show(200, truncate = false)
    lines = t1.collect()
      .mkString("\n\t\t")
  }

  def getColor(ids: String): String = {
    ids match {
      case "A" => "blue"
      case "B" => "red"
      case "C" => "cyan"
      case "D" => "magenta"
      case "E" => "orange"
      case "F" => "green"
      case "G" => "gray"
      case "H" => "violet"
      case "I" => "olive"
      case "J" => "brown"
      case "K" => "purple"
      case "L" => "black"
    }
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
