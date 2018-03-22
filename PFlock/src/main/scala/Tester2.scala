import org.apache.spark.sql.functions._
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
  var n: Long = 0
  var m: Long = 0

  case class ST_Point(id: Long, x: Double, y: Double, t: Int)

  case class Link(t1: Int, ids1: String, t2: Int, ids2: String)

  case class Disk(t: Int, ids: String, x: Double, y: Double)

  case class Flock(start: Int, end: Int, ids: String, x: Double, y: Double)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Tester")
      .getOrCreate()
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
    var links = spark.sparkContext.emptyRDD[Link].toDS()

    val C_0 = T.filter(p => timestamps.head == p.t).map(p => Disk(p.t, getLetter(p.id), p.x, p.y))
    nodes = nodes.union(C_0) // if timestamp is first or last in window
    val C_1 = T.filter(p => timestamps(1) == p.t).map(p => Disk(p.t, getLetter(p.id), p.x, p.y))
    nodes = nodes.union(C_1) // if timestamp is first or last in window
    setAxis(spark, nodes)
    setPoints(spark, nodes)
    LaTeX.write(parser("Maximal disks"))

    links = links.union(join(spark, nodes, C_1))
    setLines(spark, links)
    LaTeX.write(parser("Join"))

    LaTeX.close()
    spark.close
  }

  def join(spark: SparkSession, nodes: Dataset[Disk], C: Dataset[Disk]): Dataset[Link] = {
    import spark.implicits._
    nodes.select("t", "ids").toDF("t1", "ids")
      .join(C.select("t", "ids").toDF("t2", "ids"), "ids")
      .select("t1", "t2", "ids")
      .map{ l =>
        val t1  = l.getInt(0)
        val t2  = l.getInt(1)
        val ids = l.getString(2)
        Link(t1,ids,t2,ids)
      }.as[Link]
      .filter(l => s"${l.ids1} ${l.t1}" != s"${l.ids2} ${l.t2}")
  }

  def parser(phase: String): String = {
    val header  = parserHeader(phase)
    val content = s"$axis\n\t\t$points\n\t\t$lines"
    val footer  = parserFooter()

    s"$header$content$footer"
  }

  def parserHeader(phase: String): String = {
    val title = s"$phase..."
    val section = s"\n\\section*{$title}\n\t\\vspace{1cm}\n"
    val scale = s"\t\\psscalebox{1.0 1.0} {\n"
    val begin = s"\t\\begin{pspicture}(0,0.0)($m,$n)\n\t\t"

    s"$section$scale$begin"
  }

  def parserFooter(): String = {
    "\n\t\\end{pspicture}\n}\n\\clearpage\n"
  }

  def setAxis(spark: SparkSession, nodes: Dataset[Disk]): Unit = {
    import spark.implicits._
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

  def setPoints(spark: SparkSession, nodes: Dataset[Disk]): Unit = {
    import spark.implicits._
    points = nodes.map { t =>
        val name = s"${t.ids} ${t.t}"
        val coords = s"${t.x},${t.y}"
        s"\\dotnode[dotstyle=*,linecolor=${getColor(t.ids)},dotsize=$DotSize] ($coords) {$name}"
      }
      .collect()
      .mkString("\n\t\t")
  }

  def setLines(spark: SparkSession, links: Dataset[Link]): Unit = {
    import spark.implicits._
    lines = links.map { l =>
        val node1 = s"${l.ids1} ${l.t1}"
        val node2 = s"${l.ids2} ${l.t2}"
        s"\\ncline[linecolor=${getColor(l.ids1)},linestyle=$LineStyle]{$node1}{$node2}"
      }
      .collect()
      .mkString("\n\t\t")
  }

  def checkConsecutiveness(dots: List[String], psline: String, times: List[Int]): String = {
    val lines = dots.map(_.replace("(", "").replace(")", "").split(","))
      .map(m => (m.head.toDouble, m.last.toDouble))
      .sliding(2).toList
      .map(l => if (times.indexOf(l.head._1) + 1 == times.indexOf(l.last._1)) (l.head, l.last) else ((-1, -1), l.last))
      .sortBy(_._2._1).map(m => List(m._1, m._2)).reduce(_ union _)
      .mkString(";").split(";\\(-1,-1\\);").map(_.split(";").filter(_ != "(-1,-1)").distinct).filter(_.length > 1)
      .map(p => s"$psline ${p.mkString("")}")
    lines.mkString(" ")
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
