import SPMF.AlgoFPMax
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._

object BasicSpatialOps {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  val epsilon = 10.0
  val precision = 0.01
  val master = "local[*]" //"spark://169.235.27.134:7077"
  val cores = 4
  val partitions = 32
  val filename = "/home/and/Documents/PhD/Research/tmp/test2.txt"
  var nLines: Int = 100
  var timer = 0.0
  var clock = 0.0

  case class Disk(t: Int, ids: String, x: Double, y: Double)
  case class KeyValue(key: String, value: List[String])

  def main(args: Array[String]): Unit = {
    timer = System.currentTimeMillis()
    val simba = SimbaSession.builder().master(master)
        .appName("Benchmark")
        .config("simba.index.partitions", "16")
        .getOrCreate()
    logger.info("%-50s [%.2fs]".format("Starting session", (System.currentTimeMillis() - timer)/1000.0))

    getMaxSpeed(simba)

    simba.stop()
  }

  private def getTrajectoryLength(simba: SimbaSession): Unit = {
    import simba.implicits._

    timer = System.currentTimeMillis()
    val U = simba.sparkContext.
      textFile("/home/and/Documents/PhD/Research/Datasets/Berlin/berlin0-10.tsv").
      map { line =>
        val lineArray = line.split("\t")
        val ids = lineArray(0).trim
        val x = lineArray(1).toDouble
        val y = lineArray(2).toDouble
        val t = lineArray(3).toInt
        Disk(t, ids, x, y)
      }.toDS() //NO CACHE!!!
    var nU = U.count()
    U.show(truncate = false)
    logger.info("%-50s [%.2fs] [%d records]".format("Reading data", (System.currentTimeMillis() - timer)/1000.0, nU))

    timer = System.currentTimeMillis()
    U.groupBy("ids")
  }

  private def getMaxSpeed(simba: SimbaSession): Unit = {
    import simba.implicits._
    timer = System.currentTimeMillis()
    val U = simba.sparkContext.
      textFile("/home/and/Documents/PhD/Research/Datasets/Berlin/berlin0-10.tsv").
      map { line =>
        val lineArray = line.split("\t")
        val ids = lineArray(0).trim
        val x = lineArray(1).toDouble
        val y = lineArray(2).toDouble
        val t = lineArray(3).toInt
        Disk(t, ids, x, y)
      }.toDS() //NO CACHE!!!
    var nU = U.count()
    U.show(truncate = false)
    logger.info("%-50s [%.2fs] [%d records]".format("Reading data", (System.currentTimeMillis() - timer)/1000.0, nU))

    import java.lang.Math._
    U.filter("t = 0" ).select("ids", "x", "y").toDF("id", "x1", "y1")
      .join(U.filter("t = 10").select("ids", "x", "y").toDF("id", "x2", "y2"), "id")
      .select("id", "x1", "y1", "x2", "y2")
      .map{ p =>
        val x1 = p.getDouble(1)
        val y1 = p.getDouble(2)
        val x2 = p.getDouble(3)
        val y2 = p.getDouble(4)

        (p.getString(0), sqrt( pow(x1 - x2, 2) +  pow(y1 - y2, 2) ))
      }
      .toDF("id", "speed").orderBy(desc("speed")).show()
  }

  private def runFunction(simba: SimbaSession): Unit = {
    timer = System.currentTimeMillis()
    import simba.implicits._
    logger.info("%-50s [%.2fs]".format("Setting variables", (System.currentTimeMillis() - timer)/1000.0))

    timer = System.currentTimeMillis()
    val U = simba.sparkContext.
      textFile(filename).
      map { line =>
        val lineArray = line.split(",")
        val t = lineArray(0).toInt
        val ids = lineArray(1).trim
        val x = lineArray(2).toDouble
        val y = lineArray(3).toDouble
        Disk(t, ids, x, y)
      }.toDS() //NO CACHE!!!
    var nU = U.count()
    U.show(nLines, truncate = false)
    logger.info("%-50s [%.2fs] [%d records]".format("Reading data", (System.currentTimeMillis() - timer)/1000.0, nU))

    timer = System.currentTimeMillis()
    val M = runFPMax(U.map(_.ids), simba)
    val nM = M.count()
    M.show(nLines, truncate = false)
    logger.info("%-50s [%.2fs] [%d records]".format("Maximal disks", (System.currentTimeMillis() - timer)/1000.0, nM))

    /*
    timer = System.currentTimeMillis()
    U.index(RTreeType, "uRT", Array("x", "y")).cache()
    nU = U.count()
    U.show(nLines, truncate = false)
    logger.info("%-50s [%.2fs] [%d records]".format("Indexing U", (System.currentTimeMillis() - timer)/1000.0, nU))
    */

    timer = System.currentTimeMillis()
    val U_temp = U.map(_.ids)
      .mapPartitions(runFPMax) // Running local...
      .repartition(1)
      .mapPartitions(runFPMax) // Running global...
      .repartition(partitions).cache()
    val nU_temp = U_temp.count()
    U_temp.show(nLines, truncate = false)
    logger.info("%-50s [%.2fs] [%d records]".format("Mapping partitions", (System.currentTimeMillis() - timer)/1000.0, nU_temp))
  }

  def reduceByUnion(a: (String, List[String]), b: (String, List[String])): (String, List[String]) ={
    (a._1, a._2.union(b._2))
  }

  def reduceByFPMax(a: List[String], b: List[String]): List[String] ={
    val transactions = a.union(b).map { disk => disk.split(" ").map(new Integer(_)).toList.asJava}.asJava
    val algorithm = new AlgoFPMax
    val maximals = algorithm.runAlgorithm(transactions, 1)

    maximals.getItemsets(1).asScala.map(m => m.asScala.toList.sorted.mkString(" ")).toList
  }

  def runFPMax(data: Dataset[String], simba: SimbaSession): Dataset[String] = {
    import simba.implicits._
    val transactions = data.collect().toList.map(disk => disk.split(" ").map(new Integer(_)).toList.asJava).asJava
    val algorithm = new AlgoFPMax
    val maximals = algorithm.runAlgorithm(transactions, 1).getItemsets(1)
      .asScala.map(m => m.asScala.toList.sorted.mkString(" ")).toList

    simba.sparkContext.parallelize(maximals).toDS()
  }

  def runFPMax(data: Iterator[String]): Iterator[String] = {
    val transactions = data.toList.map(disk => disk.split(" ").map(new Integer(_)).toList.asJava).asJava
    val algorithm = new AlgoFPMax

    algorithm.runAlgorithm(transactions, 1).getItemsets(1).asScala.map(m => m.asScala.toList.sorted.mkString(" ")).toList.toIterator
  }
}
