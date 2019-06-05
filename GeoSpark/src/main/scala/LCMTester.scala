import org.slf4j.{LoggerFactory, Logger}
import java.io.PrintWriter
import scala.collection.mutable
import scala.io.Source
import scala.language.postfixOps
import sys.process._

object LCMTester {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class SimplePoint(x: Double, y: Double, pids: String)

  def main(args: Array[String]): Unit = {
    val lines = Source.fromFile("/home/and/Documents/PhD/Research/GeoSpark/src/main/scala/SPMF/pids_and_points.tsv")
    logger.info("Reading file...")
    val points = lines.getLines().map{ line =>
      val pids_point = line.split("\t")
      val pids = pids_point(0).split(" ").map(_.toInt).sorted.toList.mkString(" ")
      val x    = pids_point(1).toDouble
      val y    = pids_point(2).toDouble
      SimplePoint(x, y, pids)
    }
    
    logger.info(points.map(p => s"${p.toString()}\n").mkString(""))

    //logger.info("Running LCM...")
  }
}

