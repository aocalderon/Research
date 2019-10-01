import org.rogach.scallop.{ScallopConf, ScallopOption}
import java.util.{Timer, TimerTask}
import java.sql.Timestamp
import scala.io.Source
import java.io.PrintWriter
import org.slf4j.{Logger, LoggerFactory}

object TrajectorySource {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    val params = new TSConf(args)
    val input = params.input()
    val output = params.output()
    val tag = params.tag()
    val separator = params.sep()
    val extension = params.ext()
    val n = params.n()
    val timer = new Timer()

    var counter = params.count()
    timer.scheduleAtFixedRate( new TimerTask {
      override def run() = {
        if(counter <= n){
          val ts = new Timestamp(System.currentTimeMillis())
          ts.setNanos(0)
          val A = s"${input}${tag}${separator}${counter}.${extension}"
          val B = s"${output}${tag}${separator}${counter}.${extension}"

          val in = Source.fromFile(A)
          val content = in.getLines.map{ line => 
            val arr = line.split("\t")
            val t = arr(0).toLong
            val x = arr(1).toDouble
            val y = arr(2).toDouble
            val pids = arr(3)

            s"$t\t$x\t$y\t$pids\t$ts\n"
          }.mkString("")
          in.close()

          val out = new PrintWriter(B)
          out.write(content)
          out.close()

          val filename = s"${tag}${separator}${counter}.${extension}"
          logger.info(s"$filename has been copied in $output at $ts")
          counter = counter + 1
        } else {
          timer.cancel()
        }
      }
    }, params.start(), params.rate())
  }
}

class TSConf(args: Seq[String]) extends ScallopConf(args) {
  val input:  ScallopOption[String] = opt[String] (default = Some("/home/acald013/Datasets/ICPE/Demo/in/"))
  val output: ScallopOption[String] = opt[String] (default = Some("/home/acald013/Datasets/ICPE/Demo/out/"))
  val tag:    ScallopOption[String] = opt[String] (default = Some("LA"))
  val sep:    ScallopOption[String] = opt[String] (default = Some("_"))
  val ext:    ScallopOption[String] = opt[String] (default = Some("tsv"))
  val start:  ScallopOption[Long]   = opt[Long]   (default = Some(0L))
  val rate:   ScallopOption[Long]   = opt[Long]   (default = Some(1000L))
  val n:      ScallopOption[Int]    = opt[Int]    (default = Some(5))
  val count:  ScallopOption[Int]    = opt[Int]    (default = Some(0))
  
  verify()
}
