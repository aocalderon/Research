package SPMF.ScalaLCM

import java.util
import SPMF.{AlgoLCM, Transactions}
import org.slf4j.{Logger, LoggerFactory}
import java.io._
import scala.io.Source
import scala.collection.JavaConverters._


object LCMjava{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val mu: Int = 1

  def main(args: Array[String]): Unit = {
    val input_file = args(0)
    val D = Source.fromFile("/tmp/LCMjavaLocalIn_0.txt").getLines.map(_.split(" ").map(new Integer(_)).sorted.toList.asJava).toList.asJava
    val lcm = new AlgoLCM
    val T = new Transactions(D)
    val maximals = lcm.runAlgorithm(1, T).getItemsets(mu).asScala.map(_.asScala.toList.map(_.toLong).sorted.mkString(" ")).sorted
    maximals.foreach(println)
  }
}
