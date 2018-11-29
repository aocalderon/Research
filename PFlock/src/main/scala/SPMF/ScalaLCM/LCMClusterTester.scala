package SPMF.ScalaLCM

import java.util

import SPMF.{AlgoFPMax, AlgoLCM2}
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.slf4j.{Logger, LoggerFactory}
import java.io._

import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._


object LCMClusterTester{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  case class DiskFormat(partition_id: String, point_ids: String)
  case class Disk(point_ids: String, x: Double, y: Double)
  //case Disk(points_ids: String)

  def main(args: Array[String]): Unit = {
    val conf = new LCMClusterTesterConf(args)
    val filename   = conf.input()
    val master     = conf.master()
    val partitions = conf.partitions()
    val cores      = conf.cores()
    val mu         = conf.mu()
    
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
    val data = simba.read.option("header", "false")
      .csv(filename)
      .map(d => Disk(d.getString(0), d.getString(1).trim.toDouble, d.getString(2).trim.toDouble))
      .distinct()
      .index(RTreeType, "dRT", Array("x", "y"))
      .cache()
    val nData = data.count()
    log("Reading file...", timer, s"$nData records.")

    //Collecting info about partitions...
    timer = System.currentTimeMillis()
    val partitionsInfo = data.select("point_ids").distinct().rdd.mapPartitionsWithIndex{ (partition_id, disks) =>
      disks.map(d => (partition_id, d.getString(0).split(" ").map(_.toInt).sorted.mkString(" "))).toIterator
    }.collect().sortBy(_._2).sortBy(_._1)
    //saveToFile(partitionsInfo, s"Partitions_${partitions}_$cores.txt")
    val nPartitionsInfo = partitionsInfo.map(_._1).distinct.size
    log("Collecting info about partititons...", timer, s"$nPartitionsInfo partitions")

    // Finding local maximal patterns FPMax...
    timer = System.currentTimeMillis()
    val FPMAXlocal = data.rdd.mapPartitionsWithIndex{ (partition_id, disks) =>
      val transactions = disks.map( candidate => candidate.point_ids.split(" ").map(new Integer(_)).toList.asJava ).toList.asJava
      //saveToFile(transactions, s"/tmp/FPMAXlocal_${partitions}_${cores}_$partition_id.txt")
      val fpmax = new AlgoFPMax
      val maximals = fpmax.runAlgorithm(transactions, 1)
      maximals.getItemsets(mu).asScala.map(m => (partition_id, m.asScala.toList.map(_.toLong).sorted)).toIterator
    }.distinct()
    val nFPMAXlocal = FPMAXlocal.count()
    log("Finding local maximal patterns FPMax...", timer, s"$nFPMAXlocal patterns.")
    saveToFile(FPMAXlocal, s"/tmp/FPMAXglobal_${partitions}_$cores.txt")

    // Finding global maximal patterns FPMax...
    timer = System.currentTimeMillis()
    val FPMAXglobalTransactions = FPMAXlocal.map(_._2.mkString(" ").split(" ").map(new Integer(_)).toList.asJava).collect().toList.asJava
    val globalFPMAX = new AlgoFPMax
    val FPMAXglobalMaximals= globalFPMAX.runAlgorithm(FPMAXglobalTransactions, 1)
    val FPMAXglobalPatterns = FPMAXglobalMaximals.getItemsets(mu).asScala.map(_.asScala.toList.map(_.toLong).sorted).toList

    val nFPMAXglobalPatterns = FPMAXglobalPatterns.size
    log("Finding global maximal patterns FPMax...", timer, s"$nFPMAXglobalPatterns patterns.")

    // Finding local maximal patterns LCM...
    timer = System.currentTimeMillis()
    val LCMlocal = data.rdd.mapPartitionsWithIndex{ (partition_id, disks) =>
      val transactions = disks.map( candidate => candidate.point_ids.split(" ").map(new Integer(_)).toList.asJava).toList.asJava
      //saveToFile(transactions, s"LCMlocal_${partitions}_${cores}_$partition_id.txt")
      val lcm = new AlgoLCM2
      val data = new SPMF.Transactions(transactions)
      val maximals = lcm.run(data)
      maximals.asScala
        .map(m => (partition_id, m.asScala.toList.map(_.toLong).sorted))
        .toIterator
    }.distinct()
    val nLCMlocal = LCMlocal.count()
    log("Finding local maximal patterns LCM...", timer, s"$nLCMlocal patterns.")
    saveToFile(LCMlocal, s"/tmp/LCMglobal_${partitions}_$cores.txt")

    // Finding global maximal patterns LCM...
    timer = System.currentTimeMillis()
    val LCMglobalTransactions = LCMlocal.map(_._2.mkString(" ").split(" ").map(new Integer(_)).toList.asJava).collect().toList.asJava
    val globalLCM = new AlgoLCM2
    val LCMglobalData = new SPMF.Transactions(LCMglobalTransactions)
    val LCMglobalMaximals= globalLCM.run(LCMglobalData)
    val LCMglobalPatterns = LCMglobalMaximals.asScala.map(_.asScala.toList.map(_.toLong).sorted).toList

    val nLCMglobalPatterns = LCMglobalPatterns.size
    log("Finding global maximal patterns LCM...", timer, s"$nLCMglobalPatterns patterns.")
    saveToFileJava(LCMglobalPatterns, "/tmp/JavaLCMmax.txt")

    // Finding local maximal patterns Scala LCM...
    timer = System.currentTimeMillis()
    val ScalaLCMlocal = data.rdd.mapPartitionsWithIndex{ (partition_id, disks) =>
      val D = disks.map(_.point_ids.split(" ").map(_.toInt).toList).toList
      //saveToFile(D, s"ScalaLCMlocal_${partitions}_${cores}_$partition_id.txt")
      val maximals = IterativeLCMmax.run(D.map(d => new Transaction(d)))
      maximals.map(m => (partition_id, m.split(" ").map(_.toLong).sorted.toList)).toIterator
    }.distinct()
    val nScalaLCMlocal = ScalaLCMlocal.count()
    log("Finding local maximal patterns Scala LCM...", timer, s"$nScalaLCMlocal patterns.")
    saveToFile(ScalaLCMlocal, s"/tmp/ScalaLCMglobal_${partitions}_$cores.txt")

    // Finding global maximal patterns Scala LCM...
    timer = System.currentTimeMillis()
    val ScalaLCMglobalTransactions = ScalaLCMlocal.map(_._2.map(_.toString().toInt)).collect().toList
    val ScalaLCMglobalPatterns = IterativeLCMmax.run(ScalaLCMglobalTransactions.map(g => new Transaction(g)))

    val nScalaLCMglobalPatterns = ScalaLCMglobalPatterns.size
    log("Finding global maximal patterns Scala LCM...", timer, s"$nScalaLCMglobalPatterns patterns.")
    saveToFileScala(ScalaLCMglobalPatterns, "/tmp/ScalaLCMmax.txt")
  }

  def log(msg: String, timer: Long, tag: String = ""): Unit = {
    val time = (System.currentTimeMillis() - timer) / 1000.0
    if(tag == ""){
      logger.info("%-45s | %6.2f".format(msg, time))
    } else {
      logger.info("%-45s | %6.2f | %s".format(msg, time, tag))
    }
  }

  def saveToFile(t: util.List[util.List[Integer]], filename: String): Unit = {
    val pw = new PrintWriter(new File(filename))
    pw.write(t.asScala.map(_.asScala.toList.map(_.toInt).mkString(" ")).mkString("\n"))
    pw.close()
  }
  def saveToFile(d: List[List[Int]], filename: String): Unit = {
    val pw = new PrintWriter(new File(filename))
    pw.write(d.map(_.mkString(" ")).mkString("\n"))
    pw.close()
  }

  def saveToFileJava(d: List[List[Long]], filename: String): Unit = {
    val pw = new PrintWriter(new File(filename))
    pw.write(d.map(_.mkString(" ")).mkString("\n"))
    pw.close()
  }

  def saveToFileScala(d: List[String], filename: String): Unit = {
    val pw = new PrintWriter(new File(filename))
    pw.write(d.mkString("\n") ++ "\n")
    pw.close()
  }

  def saveToFile(rdd: RDD[(Int, List[Long])], filename: String): Unit = {
    val pw = new PrintWriter(new File(filename))
    pw.write(rdd.collect().map(m => s"${m._2.mkString(" ")}").sorted.mkString("\n"))
    pw.close()
  }

  def saveToFile(data: Array[(Int, String)], filename: String): Unit = {
    val pw = new PrintWriter(new File(filename))
    for(d <- data){
      pw.write(s"${d._1},${d._2}\n")
    }
    pw.close()
  }

}

import org.rogach.scallop.{ScallopConf, ScallopOption}

class LCMClusterTesterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input:      ScallopOption[String] = opt[String] (default = Some("/home/and/tmp/transactions.txt"))
  val master:     ScallopOption[String] = opt[String] (default = Some("local[*]"))
  val partitions: ScallopOption[Int]    = opt[Int]    (default = Some(4))
  val cores:      ScallopOption[Int]    = opt[Int]    (default = Some(3))
  val mu:         ScallopOption[Int]    = opt[Int]    (default = Some(1))

  verify()
}
