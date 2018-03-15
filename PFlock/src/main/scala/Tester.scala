import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.util.Random
import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode
import collection.mutable.ListBuffer

object Tester {
	private val log: Logger = LoggerFactory.getLogger("myLogger")
  private val DotSize: Double = 0.2
  private val AxisColor: String = "black"
  private val AxisWidth: Double = 0.05
  private val jitter: Double = 0.5
  var lines = Array.empty[String]
  var axis = Array.empty[String]
  var dots = Array.empty[String]
  var n: Long = 0
  var m: Long = 0

  case class ST_Point(id: Long, x: Double, y: Double, t: Int)
  case class Flock(start: Int, end: Int, ids: String, x: Double, y: Double)
  case class PSDot(id: Long, t: Int, dot:String)

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
    val trajs = file.as[String]
      .map(traj => traj.split(" ").map(_.toInt).zip(1 to nFile).map(i => i._1 * i._2).filter(_ > 0).map(_ - 1))
      .withColumn("id", monotonically_increasing_id())
      .withColumn("t", explode($"value"))
      .select("id", "t")
      .map{ tuple =>
        val id = tuple.getLong(0)
        val t = tuple.getInt(1)
        var s = 1
        if(Random.nextBoolean()) {s = 1} else {s = -1}
        val y = id + BigDecimal(Random.nextDouble() * jitter * s).setScale(2, RoundingMode.HALF_UP).toDouble

        ST_Point(id, t, y, t)
      }.cache()
    n = trajs.select("id").distinct().count()
    log.info(s"Trajectories: $n")
    m = trajs.select("t").distinct().count()
    log.info(s"Timestamps:   $m")
    log.info("Reading trajectories...")

    val timestamps = List(4,0,2,3,1,8,6,7,5,10,9)

    val LaTeX = new ListBuffer[String]()
    var F_prime = spark.sparkContext.emptyRDD[ST_Point].toDS()

    val C_0 = trajs.filter(p => timestamps.head == p.t)
    F_prime = F_prime.union(C_0) // if timestamp is first or last in window
    val C_1 = trajs.filter(p => timestamps(1) == p.t)
    F_prime = F_prime.union(C_1) // if timestamp is first or last in window
    LaTeX += parser(spark, F_prime, "Maximal disks")
    var F = F_prime.toDF("id", "t0", "x0", "y0").join(C_1, "id").select("id", "x", "y", "t").as[ST_Point]
    F_prime = F_prime.union(F).distinct()
    LaTeX += parser(spark, F_prime, "Join")

    val C_2 = trajs.filter(p => timestamps(2) == p.t)
    LaTeX += parser(spark, F_prime.union(C_2), "Maximal disks") // Union just for visualization...
    F = F_prime.toDF("id", "t0", "x0", "y0").join(C_2, "id").select("id", "x", "y", "t").as[ST_Point]
    F_prime = F_prime.union(F)
    LaTeX += parser(spark, F_prime, "Join")

    val C_3 = trajs.filter(p => timestamps(3) == p.t)
    LaTeX += parser(spark, F_prime.union(C_3), "Maximal disks")
    F = F_prime.toDF("id", "t0", "x0", "y0").join(C_3, "id").select("id", "x", "y", "t").as[ST_Point]
    F_prime = F_prime.union(F)
    LaTeX += parser(spark, F_prime, "Join")

    val C_4 = trajs.filter(p => timestamps(4) == p.t)
    LaTeX += parser(spark, F_prime.union(C_4), "Maximal disks")
    F = F_prime.toDF("id", "t0", "x0", "y0").join(C_4, "id").select("id", "x", "y", "t").as[ST_Point]
    F_prime = F_prime.union(F)
    LaTeX += parser(spark, F_prime, "Join")

    save(LaTeX)
    log.info("Parsing LaTeX...")

		spark.close
	}

  def save(LaTeX: ListBuffer[String]): Unit ={
    new java.io.PrintWriter("/home/and/Documents/PhD/Research/Validation/LaTeX/graph.tex") {
      write(LaTeX.mkString("\n"))
      close()
    }
  }

  def parser(spark: SparkSession, trajs: Dataset[ST_Point], phase: String): String = {
    if(phase == "Maximal disks"){
      axis = parserAxis(spark, trajs)
      log.info("Drawing axis...")
      dots = parserPoints(spark, trajs)
      log.info("Drawing points...")
    }

    if(phase == "Join"){
      lines = parserLines(spark, trajs)
      log.info("Drawing lines...")
    }

    val title = s"$phase..."
    val section = s"\\section*{$title}\n\t\\vspace{1cm}\n"
    val scale = s"\t\\psscalebox{1.0 1.0} {\n"
    val begin = s"\t\\begin{pspicture}(0,0.0)($m,$n)\n\t\t"
    val content = s"${axis.union(lines).union(dots).mkString("\n\t\t")}"
    val end = "\n\t\\end{pspicture}\n}\n"

    s"$section$scale$begin$content$end\\clearpage"
  }

  def parserAxis(spark: SparkSession, trajs: Dataset[ST_Point]): Array[String] ={
    import spark.implicits._
    trajs.select("t").distinct().orderBy("t")
      .map{ t =>
        val x = t.getInt(0)
        val psline: String = s"\\psline[linecolor=$AxisColor, linewidth=$AxisWidth]"
        val uput: String = "\\uput[90](%d, %d){$t_{%d}$}".format(x, n, x)

        s"$psline ($x,-1)($x,$n) $uput"
      }
      .collect()
  }

  def parserPoints(spark: SparkSession, trajs: Dataset[ST_Point]): Array[String] ={
    import spark.implicits._
    trajs.map(t => PSDot(t.id, t.t, s"(${t.x},${t.y})"))
      .orderBy("t")
      .map{ p =>
        val psdots: String = s"\\psdots[linecolor=${getColor(p.id)}, dotsize=$DotSize]"

        s"$psdots ${p.dot}"
      }
      .collect()
  }

  def parserLines(spark: SparkSession, trajs: Dataset[ST_Point]): Array[String] ={
    import spark.implicits._
    val times = trajs.map(_.t).distinct().collect().toList.sorted
    trajs.orderBy("id", "t")
      .map(t => PSDot(t.id, t.t, s"${t.x},${t.y}"))
      .groupBy("id")
      .agg(collect_list("dot"))
      .map{ l =>
        val id = l.getLong(0)
        val psline: String = s"\\psline[linecolor=${getColor(id)}, linewidth=0.04, linestyle=dashed, dash=0.2cm 0.1cm]"
        val lines = l.getList[String](1).asScala.toList

        s"${getMultilines(lines, psline, times)}"
      }
      .collect()
  }

  def getMultilines(dots: List[String], psline: String, times: List[Int]): String = {
    val lines = dots.map(_.replace("(", "").replace(")", "").split(","))
      .map(m => (m.head.toDouble, m.last.toDouble))
      .sliding(2).toList
      .map(l => if(times.indexOf(l.head._1) + 1 == times.indexOf(l.last._1)) (l.head, l.last) else ((-1,-1), l.last))
      .sortBy(_._2._1).map(m => List(m._1, m._2)).reduce(_ union _)
      .mkString(";").split(";\\(-1,-1\\);").map(_.split(";").filter(_ != "(-1,-1)").distinct).filter(_.length > 1)
      .map(p => s"$psline ${p.mkString("")}")
    lines.mkString(" ")
  }

  def filterPhase(spark: SparkSession, trajs: Dataset[ST_Point]): Dataset[ST_Point] ={
    import spark.implicits._
    val times = trajs.map(_.t).distinct().collect().toList.sorted
    trajs.orderBy("id", "t")
      .map(t => (t.id, t.t, t.x, t.y))
      .groupBy("id")
      .agg(collect_list("dot"))
      .map{ l =>
        val id = l.getLong(0)
        val psline: String = s"\\psline[linecolor=${getColor(id)}, linewidth=0.04, linestyle=dashed, dash=0.2cm 0.1cm]"
        val lines = l.getList[String](1).asScala.toList

        s"${getMultilines(lines, psline, times)}"
      }
      .collect()

    trajs
  }

  def checkConsecutiveness(dots: List[String], psline: String, times: List[Int]): String = {
    val lines = dots.map(_.replace("(", "").replace(")", "").split(","))
      .map(m => (m.head.toDouble, m.last.toDouble))
      .sliding(2).toList
      .map(l => if(times.indexOf(l.head._1) + 1 == times.indexOf(l.last._1)) (l.head, l.last) else ((-1,-1), l.last))
      .sortBy(_._2._1).map(m => List(m._1, m._2)).reduce(_ union _)
      .mkString(";").split(";\\(-1,-1\\);").map(_.split(";").filter(_ != "(-1,-1)").distinct).filter(_.length > 1)
      .map(p => s"$psline ${p.mkString("")}")
    lines.mkString(" ")
  }

  def getColor(id: Long): String ={
    id match {
      case 0 => "blue"
      case 1 => "red"
      case 2 => "cyan"
      case 3 => "magenta"
      case 4 => "orange"
      case 5 => "green"
      case 6 => "gray"
      case 7 => "violet"
      case 8 => "olive"
      case 9 => "brown"
      case 10 => "purple"
      case 11 => "black"
    }
  }

  def getLetter(id: Long): String ={
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
