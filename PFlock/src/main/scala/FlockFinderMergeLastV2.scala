import SPMF.AlgoFPMax
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.collection.mutable

object FlockFinderMergeLastV2 {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  var conf: Conf = new Conf(Array.empty[String])

  case class ST_Point(id: Int, x: Double, y: Double, t: Int)
  case class Flock(start: Int, end: Int, ids: String, x: Double = 0.0, y: Double = 0.0)

  // Starting a session...
  var timer = System.currentTimeMillis()
  val simba = SimbaSession.builder()
    .master("local[*]")
    .appName("FlockFinder")
    .config("simba.index.partitions", 32)
    .config("spark.cores.max", 4)
    .getOrCreate()
  simba.sparkContext.setLogLevel(conf.logs())
  import simba.implicits._
  import simba.simbaImplicits._
  var flocks: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS
  var nFlocks: Long = 0
  logging("Starting session", timer)

  def run(): Unit = {
    // Setting paramaters...
    var timer = System.currentTimeMillis()
    val epsilon: Double = conf.epsilon()
    val mu: Int = conf.mu()
    val delta: Int = conf.delta()
    val partitions: Int = conf.partitions()
    val cores: Int = conf.cores()
    val print: Boolean = conf.print()
    val separator: String = conf.separator()
    val logs: String = conf.logs()
    val path: String = conf.path()
    val dataset: String = conf.dataset()
    val extension: String = conf.extension()
    var master: String = conf.master()
    if (conf.cores() == 1) {
      master = "local"
    }
    val home: String = scala.util.Properties.envOrElse(conf.home(), "/home/and/Documents/PhD/Research/")
    val point_schema = ScalaReflection
      .schemaFor[ST_Point]
      .dataType
      .asInstanceOf[StructType]
    logging("Setting paramaters", timer)

    // Reading data...
    timer = System.currentTimeMillis()
    val filename = "%s%s%s.%s".format(home, path, dataset, extension)
    val pointset = simba.read
      .option("header", "false")
      .option("sep", separator)
      .schema(point_schema)
      .csv(filename)
      .as[ST_Point]
      .cache()
    val nPointset = pointset.count()
    logging("Reading data", timer, nPointset, "points")

    // Extracting timestamps...
    timer = System.currentTimeMillis()
    val timestamps = pointset
      .map(datapoint => datapoint.t)
      .distinct
      .sort("value")
      .collect
      .toList
    val nTimestamps = timestamps.length
    logging("Extracting timestamps", timer, nTimestamps, "timestamps")

    // Running MergeLast V2.0...
    runMergeLast(pointset, timestamps)

    // Closing all...
    logger.info("Closing app...")
    simba.close()
  }

  /* MergeLast variant */
  def runMergeLast(pointset: Dataset[ST_Point], timestamps: List[Int]): Dataset[Flock] ={
    // Initialize partial result set...
    import simba.implicits._
    import simba.simbaImplicits._

    val delta = conf.delta() - 1

    for(timestamp <- timestamps.slice(0, timestamps.length - delta)){
      // Getting points at timestamp t ...
      timer = System.currentTimeMillis()
      val C_t = getMaximalDisks(pointset, timestamp)
      val nC_t = C_t.count()
      logging(s"Getting maximal disks: t=$timestamp...", timer, nC_t, "disks")

      // Getting points at timestamp t + delta ...
      timer = System.currentTimeMillis()
      val C_tPlusDelta = getMaximalDisks(pointset, timestamp + delta)
      val nC_tPlusDelta = C_tPlusDelta.count()
      logging(s"Getting maximal disks: t=$timestamp...", timer, nC_tPlusDelta, "disks")

    }

    flocks
  }

  def getMaximalDisks(pointset: Dataset[ST_Point], t: Int): Dataset[Flock] ={
    // Getting points at timestamp t ...
    var timer = System.currentTimeMillis()
    val points = pointset
      .filter(_.t == t)
      .map{ datapoint =>
        "%d\t%f\t%f".format(datapoint.id, datapoint.x, datapoint.y)
      }
      .rdd
      .cache()
    val nPoints = points.count()
    if(conf.debug()) logging(s"Getting points at t=$t...", timer, nPoints, "points")

    // Getting maximal disks at timestamp t ...
    timer = System.currentTimeMillis()
    val C: Dataset[Flock] = MaximalFinderExpansion
      .run(points, simba, conf)
      .map{ m =>
        val disk = m.split(";")
        val x = disk(0).toDouble
        val y = disk(1).toDouble
        val ids = disk(2)
        Flock(t, t, ids, x, y)
      }.toDS().cache()
    val nC = C.count()
    if(conf.debug()) logging(s"Getting maximal disks at t=$t...", timer, nC, "disks")

    C
  }

  /* SpatialJoin variant */
  def runSpatialJoin(simba: SimbaSession, timestamps: List[Int], pointset: Dataset[ST_Point], conf: Conf): Dataset[Flock] ={
    // Initialize partial result set...
    import simba.implicits._
    import simba.simbaImplicits._

    var FinalFlocks: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nFinalFlocks: Long = 0
    var F_prime: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nF_prime: Long = 0
    var U: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nU: Long = 0
    var timer: Long = 0
    val epsilon: Double = conf.epsilon()
    val mu: Int = conf.mu()
    val delta: Int = conf.delta()
    val partitions: Int = conf.partitions()
    val printIntermediate: Boolean = conf.debug()
    val speed: Double = conf.speed()
    val distanceBetweenTimestamps = speed * delta

    // For each new time instance t_i...
    for(timestamp <- timestamps){
      // Reported location for trajectories in time t_i...
      timer = System.currentTimeMillis()
      val currentPoints = pointset
        .filter(datapoint => datapoint.t == timestamp)
        .map{ datapoint =>
          "%d\t%f\t%f".format(datapoint.id, datapoint.x, datapoint.y)
        }
        .rdd
        .cache()
      val nCurrentPoints = currentPoints.count()
      logger.warn("Reported location for trajectories in time %d... [%.3fs] [%d points]".format(timestamp, (System.currentTimeMillis() - timer)/1000.0, nCurrentPoints))

      // Set of disks for t_i...
      timer = System.currentTimeMillis()
      val C: Dataset[Flock] = MaximalFinderExpansion
        .run(currentPoints, simba, conf)
        .map{ m =>
          val disk = m.split(";")
          val x = disk(0).toDouble
          val y = disk(1).toDouble
          val ids = disk(2)
          Flock(timestamp, timestamp, ids, x, y)
        }.toDS().cache()
      val nC = C.count()
      logger.warn("Set of disks for t_i... [%.3fs] [%d disks]".format((System.currentTimeMillis() - timer)/1000.0, nC))

      var nFlocks: Long = 0
      var nJoin: Long = 0
      /*****************************************************************************/
      if(nF_prime != 0) {
        // Distance Join phase with previous potential flocks...
        timer = System.currentTimeMillis()
        logger.warn("Indexing current maximal disks for timestamps %d...".format(timestamp))
        val cDS = C.index(RTreeType, "cRT", Array("lon", "lat"))
        logger.warn("Indexing previous potential flocks for timestamps %d...".format(timestamp))
        val fDS = F_prime.index(RTreeType, "f_primeRT", Array("lon", "lat"))
        logger.warn("Joining C and F_prime using a distance of %.2fm...".format(distanceBetweenTimestamps))
        val join = fDS.distanceJoin(cDS, Array("lon", "lat"), Array("lon", "lat"), distanceBetweenTimestamps)
        if (printIntermediate) {
          join.show()
        }
        nJoin = join.count()
        logger.warn("Distance Join phase with previous potential flocks... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer) / 1000.0, nJoin))

        // At least mu...
        timer = System.currentTimeMillis()
        val the_mu = conf.mu()
        val U_prime = join.map { tuple =>
          val ids1 = tuple.getString(7).split(" ").map(_.toLong)
          val ids2 = tuple.getString(2).split(" ").map(_.toLong)
          val u = ids1.intersect(ids2)
          val length = u.length
          val s = tuple.getInt(0) // set the initial time...
        val e = timestamp // set the final time...
        val ids = u.sorted.mkString(" ") // set flocks ids...
        val x = tuple.getDouble(8)
          val y = tuple.getDouble(9)
          (Flock(s, e, ids, x, y), length)
        }
          .filter(flock => flock._2 >= the_mu)
          .map(_._1)
        U = pruneSubsets(U_prime, simba).cache()
        nU = U.count()
        logger.warn("At least mu... [%.3fs] [%d candidate flocks]".format((System.currentTimeMillis() - timer) / 1000.0, nU))
      } else {
        U = C
        nU = nC
      }
      // Found flocks...
      timer = System.currentTimeMillis()
      val flocks = U.filter(flock => flock.end - flock.start + 1 == delta).cache()
      nFlocks = flocks.count()
      logger.warn("Found flocks... [%.3fs] [%d flocks]".format((System.currentTimeMillis() - timer)/1000.0, nFlocks))

      if(printIntermediate){
        logger.warn("\n\nFound %d flocks on timestamp %d:\n%s\n\n".format(nFlocks, timestamp, flocks.collect().mkString("\n")))
      }

      // Report flock patterns...
      timer = System.currentTimeMillis()
      FinalFlocks = FinalFlocks.union(flocks).cache()
      nFinalFlocks = FinalFlocks.count()
      logger.warn("Report flock patterns... [%.3fs]".format((System.currentTimeMillis() - timer)/1000.0))

      // Update u.t_start. Shift the time...
      timer = System.currentTimeMillis()
      val F = U.filter(flock => flock.end - flock.start + 1 != delta)
        .union(flocks.map(u => Flock(u.start + 1, u.end, u.ids, u.x, u.y)))
        .cache()
      val nF = F.count()
      logger.warn("Update u.t_start. Shift the time... [%.3fs] [%d flocks updated]".format((System.currentTimeMillis() - timer)/1000.0, nF))

      // Merge potential flocks U and disks C and adding to F...
      timer = System.currentTimeMillis()
      F_prime = F.union(C)
        .rdd
        .map(f => (f.ids, f))
        .reduceByKey( (a,b) => if(a.start < b.start) a else b )
        .map(_._2)
        .toDS()
        .cache()
      nF_prime = F_prime.count()
      logger.warn("Merge potential flocks U and disks C and adding to F... [%.3fs] [%d flocks added]".format((System.currentTimeMillis() - timer)/1000.0, nF_prime))
      /*****************************************************************************/

      // Reporting summary...
      logger.warn("\n\nPFLOCK_SJ\t%d\t%.1f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n"
        .format(timestamp, epsilon, mu, delta, nFlocks, nFinalFlocks, nCurrentPoints, nC, nJoin, nU, nF_prime))
    }

    FinalFlocks
  }

  private def pruneSubsets(flocks: Dataset[Flock], simba: SimbaSession): Dataset[Flock] = {
    import simba.implicits._
    import simba.simbaImplicits._

    pruneIDsSubsets(flocks, simba).rdd
      .map(f => (f.ids, f))
      .reduceByKey{ (a,b) =>
        if(a.start < b.start){ a } else { b }
      }
      .map(_._2)
      .toDS()
  }

  private def pruneIDsSubsets(flocks: Dataset[Flock], simba: SimbaSession): Dataset[Flock] = {
    import simba.implicits._
    import simba.simbaImplicits._

    val partitions = flocks.rdd.getNumPartitions
    flocks.map(_.ids)
      .mapPartitions(runFPMax) // Running local...
      .repartition(1)
      .mapPartitions(runFPMax) // Running global...
      .toDF("ids")
      .join(flocks, "ids") // Getting back x and y coordiantes...
      .select("interval", "ids", "x", "y").as[Flock]
      .groupBy("interval", "ids") // Filtering flocks with same IDs...
      .agg(min("x").alias("x"), min("y").alias("y"))
      .repartition(partitions) // Restoring to previous number of partitions...
      .as[Flock]
  }

  def runFPMax(data: Iterator[String]): Iterator[String] = {
    val transactions = data.toList.map(disk => disk.split(" ").map(new Integer(_)).toList.asJava).asJava
    val algorithm = new AlgoFPMax

    algorithm.runAlgorithm(transactions, 1).getItemsets(1).asScala.map(m => m.asScala.toList.sorted.mkString(" ")).toList.toIterator
  }

  def logging(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    logger.info("%-50s | %6.2fs | %6d %s".format(msg, (System.currentTimeMillis() - timer)/1000.0, n, tag))
  }

  def showDisks(f: Dataset[Flock]): Unit = {
    val n = f.count()
    f.orderBy("start", "ids").show(n.toInt, truncate = false)
    println(s"Number of flocks: $n")
  }


  def saveFlocks(flocks: RDD[Flock], filename: String): Unit = {
    new java.io.PrintWriter(filename) {
      write(
        flocks.map{ f =>
          "%d, %d, %s, %.3f, %.3f\n".format(f.start, f.end, f.ids, f.x, f.y)
        }.collect.mkString("")
      )
      close()
    }
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting app...")
    conf = new Conf(args)
    FlockFinderMergeLastV2.run()
  }
}
