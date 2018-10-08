package SPMF.ScalaLCM

import SPMF.{AlgoFPMax, AlgoLCM, Transactions}
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._


object Tester{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  case class DiskFormat(partition_id: String, point_ids: String)
  case class Disk(point_ids: String)

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val filename   = conf.input()
    val master     = conf.master()
    val partitions = conf.partitions()
    val cores      = conf.cores()
    val mu         = conf.mu()
    val sample     = conf.sample()

    println(s"master=$master partitions=$partitions cores=$cores")
    val simba = SimbaSession.builder()
      .master(master)
      .appName("Tester")
      .config("simba.index.partitions", partitions)
      .config("spark.cores.max", cores)
      .getOrCreate()
    import simba.implicits._
    import simba.simbaImplicits._

    // Reading file...
    var timer = System.currentTimeMillis()
    val disk_schema = ScalaReflection.schemaFor[DiskFormat].dataType.asInstanceOf[StructType]
    val data = simba.read.option("header", "false")
      .csv(filename)
      .map(d => Disk(d.getString(1)))
      .sample(false, sample, 42)
      .repartition(partitions)
      .cache()
    val nData = data.count()
    log("Reading file...", timer, s"$nData records.")
    logger.info(s"Number of partitions: ${data.rdd.getNumPartitions}")

    // Finding maximal patterns FPMax...
    timer = System.currentTimeMillis()
    val FPMAXpatterns = data.rdd.mapPartitionsWithIndex{ (partition_id, disks) =>
      val transactions = disks
        .map( candidate => candidate.point_ids.split(" ").map(new Integer(_)).toList.asJava )
        .toList
        .asJava
      val fpmax = new AlgoFPMax
      val maximals = fpmax.runAlgorithm(transactions, 1)
      maximals.getItemsets(mu)
        .asScala
        .map(m => (partition_id, m.asScala.toList.map(_.toLong).sorted))
        .toIterator
    }
    val nFPMAXpatterns = FPMAXpatterns.count()
    log("Finding maximal patterns FPMax...", timer, s"$nFPMAXpatterns patterns.")

    // Finding maximal patterns LCM...
    timer = System.currentTimeMillis()
    val LCMpatterns = data.rdd.mapPartitionsWithIndex{ (partition_id, disks) =>
      val transactions = disks
        .map( candidate => candidate.point_ids.split(" ").map(new Integer(_)).toList.asJava )
        .toSet
        .asJava
      val lcm = new AlgoLCM
      val data = new Transactions(transactions)
      val maximals = lcm.runAlgorithm(1, data)
      maximals.getItemsets(mu)
        .asScala
        .map(m => (partition_id, m.asScala.toList.map(_.toLong).sorted))
        .toIterator
    }
    val globalTransactions = LCMpatterns.map(_._2.mkString(" ").split(" ").map(new Integer(_)).toList.asJava).collect().toSet.asJava
    val globalLCM = new AlgoLCM
    val globalData = new SPMF.Transactions(globalTransactions)
    val globalMaximals= globalLCM.runAlgorithm(1, globalData)
    val globalPatterns = globalMaximals.getItemsets(mu).asScala.map(_.asScala.toList.map(_.toLong).sorted).toList

    val nGlobalPatterns = globalPatterns.size
    log("Finding maximal patterns LCM...", timer, s"$nGlobalPatterns patterns.")
  }

  def log(msg: String, timer: Long, tag: String = ""): Unit = {
    val time = (System.currentTimeMillis() - timer) / 1000.0
    if(tag == ""){
      logger.info("%-40s | %6.2f".format(msg, time))
    } else {
      logger.info("%-40s | %6.2f | %s".format(msg, time, tag))
    }
  }
}

import org.rogach.scallop.{ScallopConf, ScallopOption}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input:      ScallopOption[String] = opt[String] (required = true)
  val master:     ScallopOption[String] = opt[String] (default = Some("local[*]"))
  val partitions: ScallopOption[Int]    = opt[Int]    (default = Some(1024))
  val cores:      ScallopOption[Int]    = opt[Int]    (default = Some(7))
  val mu:         ScallopOption[Int]    = opt[Int]    (default = Some(3))
  val sample:     ScallopOption[Double] = opt[Double] (default = Some(1.0))

  verify()
}
