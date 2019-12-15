import SPMF.{AlgoFPMax, AlgoLCM, Transactions}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.simba.index.{RTree, RTreeType}
import org.apache.spark.sql.simba.partitioner.STRPartitioner
import org.apache.spark.sql.simba.spatial.{MBR, Point}
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object FPMaxTester {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val precision: Double = 0.001
  private val dimensions: Int = 2
  private val sampleRate: Double = 0.01
  private var phd_home: String = ""
  private var nPoints: Long = 0

  case class Candidate(id: Long, x: Double, y: Double, items: String)

  def test(candidates: List[String], simba: SimbaSession): Dataset[String] = {
    import simba.implicits._
    import simba.simbaImplicits._
    
    val transactions = candidates
      .map { candidate =>
        val items = candidate.split(" ")
        items.map(new Integer(_))
        .toList.asJava
      }.toList.asJava
    val algorithm = new AlgoFPMax
    val maximals = algorithm.runAlgorithm(transactions, 1)
    
    maximals.getItemsets(5)
      .asScala
      .map(m => (m.asScala.toList.map(_.toLong).sorted.mkString(" ")))
      .toDS()
  }

  def main(args: Array[String]): Unit = {
    // Reading arguments from command line...
    val conf = new Conf(args)
    val master = conf.master()
    // Starting session...
    var timer = System.currentTimeMillis()
    val simba = SimbaSession.builder()
      .master(master)
      .appName("MaximalFinderExpansion")
      .config("simba.index.partitions",conf.partitions().toString)
      .config("spark.cores.max",conf.cores().toString)
      .getOrCreate()
    import simba.implicits._
    import simba.simbaImplicits._
    //logger.info("Starting session... [%.3fs]".format((System.currentTimeMillis() - timer)/1000.0))
    // Reading...
    timer = System.currentTimeMillis()
    phd_home = scala.util.Properties.envOrElse(conf.home(), "/home/and/Documents/PhD/Research/")
    val filename = "%s%s%s.%s".format(phd_home, conf.path(), conf.dataset(), conf.extension())
    val lines = simba.sparkContext.
      textFile(filename).
      cache()
    case class Line(pid: Int, items: String)
    import simba.implicits._
    val partitions = lines.map(_.split(","))
      .map(l => (l(0).trim.toInt, l(1).trim))
      .toDF("pid", "items")
      .groupBy("pid").agg(collect_list("items"))
      .cache()
    val nPartitions = partitions.count()
    val datasets = partitions.filter($"pid" < nPartitions).map(p => (p.getInt(0), p.getList[String](1).asScala.toList.mkString("\n")))
    //logger.info(s"Number of partitions: $nPartitions")
    // Running MaximalFinder...
    //logger.info("Lauching MaximalFinder at %s...".format(DateTime.now.toLocalTime.toString))
    val start = System.currentTimeMillis()
    //println("\n")
    
    for(dataset <- datasets.collect()){
      //println("\n")
      val p = dataset._1
      //logger.info(s"FPMax for partition # $p start...")
      val start = System.currentTimeMillis()
      val data = dataset._2.split("\n").toList
      val lengths = data.map(_.split(" ").length)
      val max = lengths.max
      val min = lengths.min
      val mean = lengths.reduce(_ + _) / lengths.length * 1.0
      val patterns = FPMaxTester.test(data, simba)
      val nPatterns = patterns.count()
      val end = System.currentTimeMillis()
      //logger.info(s"FPMax for partition # $p end...")
      //patterns.show(5, false)
      println(s"Record,$p,$nPatterns,$max,$min,$mean,${(end - start)/1000.0}")
    }

    val end = System.currentTimeMillis()
    logger.info("Finishing MaximalFinder at %s...".format(DateTime.now.toLocalTime.toString))
    logger.info("Total time for MaximalFinder: %.3fms...".format((end - start)/1000.0))
    // Closing session...
    timer = System.currentTimeMillis()
    simba.close
    logger.info("Closing session... [%.3fs]".format((System.currentTimeMillis() - timer)/1000.0))
  }
}
