import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.util.Random
import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode
import collection.mutable.ListBuffer

object FlockFinderExample {
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

  case class ST_Point(id: String, x: Double, y: Double, t: Int)
  case class Flock(start: Int, end: Int, ids: String, x: Double, y: Double, times: String = "")
  case class PSDot(id: String, t: Int, dot:String)

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

        ST_Point(getLetter(id), t, y, t)
      }.cache()
    val nTrajs = trajs.count()
    n = trajs.select("id").distinct().count()
    log.info(s"Trajectories: $n")
    m = trajs.select("t").distinct().count()
    log.info(s"Timestamps:   $m")
    log.info("Reading trajectories...")

    val timestamps = List(4,0,2,3,1,8,6,7,5,10,9)

    val LaTeX = new ListBuffer[String]()
    var F_prime = spark.sparkContext.emptyRDD[Flock].toDS()

    /* 2 */ val C_0 = trajs.filter(p => p.t == timestamps.head).map(p => Flock(p.t, p.t, p.id, p.x, p.y)) // Maximals
    /* 3 */ F_prime = F_prime.union(C_0) // No join required if F_prime is empty ...
    /* 4 */ // Filter is not required
    /* 5 */ // AddAndPrune is not required

    show("F_prime", timestamps.head, F_prime)

    /* 2 */ val C_1 = trajs.filter(p => p.t == timestamps(1)).map(p => Flock(p.t, p.t, p.id, p.x, p.y)) // Maximals
    /* 3 */ var F = join(spark, F_prime, C_1, timestamps.slice(0,2)) // Join
    /* 4 */ // Filter is not required
    /* 5 */ F_prime = addAndPrune(spark, F, C_0, C_1)

    show("C", timestamps(1), C_1)
    show("F", timestamps(1), F)
    show("F_prime", timestamps(1), F_prime)

    /* 2 */ var C = trajs.filter(p => p.t == timestamps(2)).map(p => Flock(p.t, p.t, p.id, p.x, p.y)) // Maximals
    /* 3 */ F = join(spark, F_prime, C, timestamps.slice(0,3)) // Join
    /* 4 */ F = filter(F, 0, 4)
    /* 5 */ F_prime = addAndPrune(spark, F, C_0, C_1)

    show("C", timestamps(2), C)
    show("F", timestamps(2), F)
    show("F_prime", timestamps(2), F_prime)

    /* 2 */ C = trajs.filter(p => p.t == timestamps(3)).map(p => Flock(p.t, p.t, p.id, p.x, p.y)) // Maximals
    /* 3 */ F = join(spark, F_prime, C, timestamps.slice(0,4)) // Join
    /* 4 */ F = filter(F, 0, 4)
    /* 5 */ F_prime = addAndPrune(spark, F, C_0, C_1)

    show("C", timestamps(3), C)
    show("F", timestamps(3), F)
    show("F_prime", timestamps(3), F_prime)
    /*

        /* 1 */ timestamp = timestamps(4)
        /* 2 */ C = trajs.filter(p => p.t == timestamp).map(p => Flock(p.t, p.t, p.id, p.x, p.y)) // Maximals
        /* 3 */ F = join(spark, F_prime, C) // Join
        /* 4 */ F = filter(F, 0, 4)
        /* 5 */ F_prime = addAndPrune(spark, F_prime, B_prime)

        show("C", timestamp, C)
        show("F", timestamp, F)
        show("F_prime", timestamp, F_prime)
    */
    spark.close
  }

  def join(spark: SparkSession, F_prime: Dataset[Flock], C: Dataset[Flock], timestamps: List[Int]): Dataset[Flock] ={
    import spark.implicits._
    val timestamp = timestamps.last
    val times = timestamps.sorted
    val index = times.indexOf(timestamp)
    var left = 0
    if (index >= 1){
      left = times(index - 1)
    }
    val right = times(index + 1)

    F_prime.toDF("s","e","ids","x0","y0","t").join(C, "ids")
      .map{ f =>
        val ids = f.getString(0)
        val s1 = f.getInt(1) // F_prime flock start...
        val e1 = f.getInt(2) // F_prime flock end...
        val x = f.getDouble(3)
        val y = f.getDouble(4)
        val t = f.getString(5).split(" ").toBuffer.filter(_ != "") // Storage of intermediate points...
        val s2 = f.getInt(6) // C flock start...
        val e2 = f.getInt(7) // C flock end (should be equal to s2)...
        t += s"$s2"

        var s = -1
        var e = -1
        if (s1 < s2 && s2 < e1){ // C flock intersects F_prime flock...
          s = s1
          e = e1
        } else if (e1 == left) { // F_prime flock is on the left...
          s = s1
          e = e2
        } else if (s1 == right){ // F_prime flock is on the right...
          s = s2
          e = e1
        }
        Flock(s, e, ids, x, y, t.mkString(" "))
      }
      .filter(_.start != -1)
      .as[Flock]
  }

  def filter(F_prime: Dataset[Flock], start: Int, end: Int): Dataset[Flock] ={
    F_prime.filter(f => f.start == start || f.end == end)
  }

  def addAndPrune(spark: SparkSession, F: Dataset[Flock], C_0: Dataset[Flock], C_1: Dataset[Flock]): Dataset[Flock] ={
    import spark.implicits._
    F.union(C_1)
      .groupBy("ids", "start")
      .agg(max("end").alias("end"), min("x").alias("x"), min("y").alias("y"), collect_list("times").alias("times"))
      .select("start","end","ids","x","y","times")
      .map{ f =>
        val s = f.getInt(0) // F_prime flock start...
        val e = f.getInt(1) // F_prime flock end...
        val ids = f.getString(2)
        val x = f.getDouble(3)
        val y = f.getDouble(4)
        val t = f.getList[String](5).asScala.mkString(" ").split(" ").distinct.mkString(" ") // Storage of intermediate points...
        Flock(s,e,ids,x,y,t)
      }
      .as[Flock]
      .union(C_0)
      .groupBy("ids", "end")
      .agg(min("start").alias("start"), min("x").alias("x"), min("y").alias("y"), collect_list("times").alias("times"))
      .select("start","end","ids","x","y","times")
      .map{ f =>
        val s = f.getInt(0) // F_prime flock start...
        val e = f.getInt(1) // F_prime flock end...
        val ids = f.getString(2)
        val x = f.getDouble(3)
        val y = f.getDouble(4)
        val t = f.getList[String](5).asScala.mkString(" ").split(" ").distinct.mkString(" ") // Storage of intermediate points...
        Flock(s,e,ids,x,y,t)
      }
      .as[Flock]
  }

  def show(msg: String, t: Int, flocks: Dataset[Flock]): Unit ={
    flocks.orderBy("ids", "start", "end").show(truncate = false)
    log.info(s"$msg timesatamp $t: ${flocks.count()}")
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
        val id = l.getString(0)
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
        val id = l.getString(0)
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

  def getColor(id: String): String ={
    id match {
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
