import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import java.io.PrintWriter
import org.andress.Utils.{debug, timer}

object TrajSplitter {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class Traj(tid: Long, t: Int, points: List[String])
  case class ST_Point(pid: Long, x: Double, y: Double, t: Int)


  def main(args: Array[String]): Unit = {
    val params = new TrajSplitterConf(args)
    val input = params.input()
    val name = input.split("/").last.split("\\.").head
    val output = params.output() match {
      case "" => input.split("/").dropRight(1).mkString("/")
      case _  => params.output()
    }

    val spark = timer{"Starting session"}{
      SparkSession.builder()
        .appName("TrajSplitter")
        .getOrCreate()
    }
    import spark.implicits._

    val points = timer{"Reading data"}{
      val points = spark.read.option("header", "false").option("delimiter", "\t").csv(input)
        .map{ traj =>
          val pid = traj.getString(0).toLong
          val x = traj.getString(1).toDouble
          val y = traj.getString(2).toDouble
          val t = traj.getString(3).toInt
          ST_Point(pid, x, y, t)
        }.orderBy($"t").cache()
      val nTrajs = points.count()
      points
    }

    timer{"Saving time instants"}{
      val ts = points.select($"t").distinct().collect().map(_.getInt(0))
      val nTs = ts.size
      val start = params.start()
      val end = params.end() match {
        case -1 => nTs - 1
        case _  => params.end()
      }
      logger.info(s"Total time intervals: ${nTs}")
      for(t <- ts if t > start && t < end){
        val sample = points.filter(_.t == t)
        val filename = s"${output}/${name}_${t}.tsv"
        val f = new java.io.PrintWriter(filename)
        f.write(sample.map(p => s"${p.pid}\t${p.x}\t${p.y}\t${p.t}\n").collect().mkString(""))
        f.close()
        logger.info(s"Saved: ${filename}")
      }
    }

    timer{"Closing session"}{
      spark.close()
    }
  }
}

class TrajSplitterConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String] (required = true)
  val output: ScallopOption[String] = opt[String] (required = true)
  val start: ScallopOption[Int] = opt[Int] (default = Some(0))
  val end: ScallopOption[Int] = opt[Int] (default = Some(-1))
  val debug: ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
