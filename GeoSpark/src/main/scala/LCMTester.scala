import org.slf4j.{LoggerFactory, Logger}
import java.io.PrintWriter
import scala.collection.mutable
import scala.io.Source
import scala.language.postfixOps
import sys.process._
import SPMF.{AlgoLCM2, Transactions, Transaction}
import scala.collection.JavaConverters._

object LCMTester {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class Disk(x: Double, y: Double, pids: String)

  def main(args: Array[String]): Unit = {
    val lines = Source.fromFile("/home/and/Documents/PhD/Research/GeoSpark/src/main/scala/SPMF/pids_and_points.tsv")
    logger.info("Reading file...")
    val points = lines.getLines().map{ line =>
      val pids_point = line.split("\t")
      val pids = pids_point(0).split(" ").map(_.toInt).sorted.toList.mkString(" ")
      val x    = pids_point(1).toDouble
      val y    = pids_point(2).toDouble
      val t = new Transaction(x, y, pids)
      t
    }.toList.asJava
    //logger.info(points.map(p => s"${p.toString()}\n").mkString(""))
    logger.info("Running LCM...")
    val lcm = new AlgoLCM2()
    val data = new Transactions(points, 0)
    val maximals = lcm.run(data).asScala.map{ maximal =>
      maximal.asScala.toList.map(_.toInt)
    }
    logger.info(maximals.map(m => s"${m.toString()}\n").mkString(""))
  }
}

