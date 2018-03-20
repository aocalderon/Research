import SPMF.AlgoFPMax
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object FlockFinderBenchmark {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(id: Int, x: Double, y: Double, t: Int)
  case class Flock(start: Int, end: Int, ids: String, x: Double = 0.0, y: Double = 0.0)

  def run(conf: Conf): Unit = {
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

    // Starting a session...
    timer = System.currentTimeMillis()
    val simba = SimbaSession.builder()
      .master(master)
      .appName("FlockFinder")
      .config("simba.index.partitions", partitions)
      .config("spark.cores.max", cores)
      .getOrCreate()
    simba.sparkContext.setLogLevel(logs)
    import simba.implicits._
    import simba.simbaImplicits._
    var flocks: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS
    var nFlocks: Long = 0
    logging("Starting session", timer)

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

    // Running SpatialJoin variant...
    logger.info("")
    logger.warn("dataset=%s,method=SpatialJoin,epsilon=%.1f,mu=%d,delta=%d".format(dataset, epsilon, mu, delta))
    logger.info("")
    timer = System.currentTimeMillis()
    flocks = runSpatialJoin(simba, timestamps, pointset, conf)
    nFlocks = flocks.count()
    if(print){ // Reporting final set of flocks...
      flocks.orderBy("start", "ids").show(100, truncate = false)
      logger.warn("\n\nFinal flocks: %d\n".format(nFlocks))
    }
    logging("Running SpatialJoin variant", timer, nFlocks, "flocks")

    // Running MergeLast variant...
    logger.info("")
    logger.warn("dataset=%s,method=MergeLast,epsilon=%.1f,mu=%d,delta=%d".format(dataset, epsilon, mu, delta))
    logger.info("")
    timer = System.currentTimeMillis()
    flocks = runMergeLast(simba, reorder(timestamps, delta), pointset, conf)
    nFlocks = flocks.count()
    if(print){ // Reporting final set of flocks...
      flocks.orderBy("start", "ids").show(100, truncate = false)
      logger.warn("Final flocks: %d".format(nFlocks))
    }
    logging("Running MergeLast variant", timer, nFlocks, "flocks")

    // Closing all...
    logger.info("Closing app...")
    simba.close()
  }

  /* Spatial Join variant */
  def runSpatialJoin(simba: SimbaSession, timestamps: List[Int], pointset: Dataset[ST_Point], conf: Conf): Dataset[Flock] ={
    // Initialize partial result set...
    import simba.implicits._
    import simba.simbaImplicits._

    var F_prime: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nF_prime: Long = 0
    var timer: Long = 0
    val epsilon: Double = conf.epsilon()
    val mu: Int = conf.mu()
    val delta: Int = conf.delta()
    val debug: Boolean = conf.debug()
    val speed: Double = conf.speed()
    val distanceBetweenTimestamps = speed * 1.0

    // For each new time instance t_i...
    for(timestamp <- timestamps){
      // Reported location for trajectories in time t_i...
      logger.info(s"Running timestamps $timestamp...")
      timer = System.currentTimeMillis()
      val points = pointset
        .filter(datapoint => datapoint.t == timestamp)
        .map{ datapoint =>
          "%d\t%f\t%f".format(datapoint.id, datapoint.x, datapoint.y)
        }
        .rdd
        .cache()
      val nPoints = points.count()
      logging("A.Reporting locations", timer, nPoints, "points")

      // Set of disks for t_i...
      timer = System.currentTimeMillis()
      val C: Dataset[Flock] = MaximalFinderExpansion
        .run(points, simba, conf)
        .map{ m =>
          val disk = m.split(";")
          val x = disk(0).toDouble
          val y = disk(1).toDouble
          val ids = disk(2)
          Flock(timestamp, timestamp, ids, x, y)
        }.toDS().cache()
      val nC = C.count()
      logging("B.Getting maximal disks", timer, nC, "disks")

      if(nF_prime == 0) { // If F_prime is empty just assign the set of maximals disks...
        F_prime = C
        nF_prime = nC
      } else {
        // Distance Join phase with previous potential flocks...
        timer = System.currentTimeMillis()
        C.index(RTreeType, "cRT", Array("x", "y")).cache()
        logging("Indexing C", timer, C.count(), "flocks")
        F_prime.index(RTreeType, "f_primeRT", Array("x", "y")).cache()
        logging("Indexing F_prime", timer, F_prime.count(), "flocks")
        val U = F_prime.distanceJoin(C, Array("x", "y"), Array("x", "y"), distanceBetweenTimestamps)
        val nU = U.count()
        if (debug) U.show(truncate = false)
        logging("C.Joining", timer, nU, "pairs")

        // At least mu...
        timer = System.currentTimeMillis()
        val mu = conf.mu()
        var F = U.map{ tuple =>
          val ids1 = tuple.getString(2).split(" ").map(_.toLong)
          val ids2 = tuple.getString(7).split(" ").map(_.toLong)
          val u = ids1.intersect(ids2)
          val length = u.length
          val ids = u.sorted.mkString(" ")  // set flocks ids...
          val s = tuple.getInt(0)  // set start time...
          val e = timestamp  // set end time...
          val x = tuple.getDouble(8)
          val y = tuple.getDouble(9)

          (Flock(s, e, ids, x, y), length)
        }
          .filter(f => f._2 >= mu)
          .map(f => f._1)
          .cache
        var nF = F.count()
        if(debug) showing(F, nF)
        F = pruneFlocks(F, simba).cache()
        nF = F.count()
        if(debug) showing(F, nF)
        logging("D.Filtering", timer, nF, "flocks")

        // Saving current flocks for next iteration...
        F_prime = F
        nF_prime = nF
      }
    }

    // Found flocks...
    timer = System.currentTimeMillis()
    val flocks = F_prime.filter(flock => flock.end - flock.start + 1 == delta).cache()
    val nFlocks = flocks.count()
    if(debug) F_prime.show(truncate = false)
    logging("Found flocks", timer, nFlocks, "flocks")

    // Reporting summary...
    logger.warn("\n\nPFLOCK_JS\t%.1f\t%d\t%d\t%d\n".format(epsilon, mu, delta, nFlocks))

    flocks.as[Flock]
  }

  /* MergeLast variant */
  def runMergeLast(simba: SimbaSession, timestamps: List[Int], pointset: Dataset[ST_Point], conf: Conf): Dataset[Flock] ={
    // Initialize partial result set...
    import simba.implicits._
    import simba.simbaImplicits._

    var F_prime: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nF_prime: Long = 0
    var timer: Long = 0
    val epsilon: Double = conf.epsilon()
    val mu: Int = conf.mu()
    val delta: Int = conf.delta()
    val debug: Boolean = conf.debug()
    val speed: Double = conf.speed()
    val distanceBetweenTimestamps = speed * delta

    // For each new time instance t_i...
    for(timestamp <- timestamps){
      // Reported location for trajectories in time t_i...
      logger.info(s"Running timestamps $timestamp...")
      timer = System.currentTimeMillis()
      val points = pointset
        .filter(datapoint => datapoint.t == timestamp)
        .map{ datapoint =>
          "%d\t%f\t%f".format(datapoint.id, datapoint.x, datapoint.y)
        }
        .rdd
        .cache()
      val nPoints = points.count()
      logging("A.Reporting locations", timer, nPoints, "points")

      // Set of disks for t_i...
      timer = System.currentTimeMillis()
      val C: Dataset[Flock] = MaximalFinderExpansion
        .run(points, simba, conf)
        .map{ m =>
          val disk = m.split(";")
          val x = disk(0).toDouble
          val y = disk(1).toDouble
          val ids = disk(2)
          Flock(timestamp, timestamp, ids, x, y)
        }.toDS().cache()
      val nC = C.count()
      logging("B.Getting maximal disks", timer, nC, "disks")

      if(nF_prime == 0) { // If F_prime is empty just assign the set of maximals disks...
        F_prime = C
        nF_prime = nC
      } else {
        // Distance Join phase with previous potential flocks...
        timer = System.currentTimeMillis()
        C.index(RTreeType, "cRT", Array("x", "y")).cache()
        logging("Indexing C", timer, C.count(), "flocks")
        F_prime.index(RTreeType, "f_primeRT", Array("x", "y")).cache()
        logging("Indexing F_prime", timer, F_prime.count(), "flocks")
        val U = F_prime.distanceJoin(C, Array("x", "y"), Array("x", "y"), distanceBetweenTimestamps)
        val nU = U.count()
        if (debug) U.show(truncate = false)
        logging("C.Joining", timer, nU, "pairs")

        // At least mu...
        timer = System.currentTimeMillis()
        val mu = conf.mu()
        var F = U.map{ tuple =>
            val ids1 = tuple.getString(2).split(" ").map(_.toLong)
            val ids2 = tuple.getString(7).split(" ").map(_.toLong)
            val u = ids1.intersect(ids2)
            val length = u.length
            val ids = u.sorted.mkString(" ")  // set flocks ids...
            val s1 = tuple.getInt(0)
            val s2 = tuple.getInt(5)
            val e1 = tuple.getInt(1)
            val e2 = tuple.getInt(6)
            val s = if(s1 < s2) s1 else s2  // set start time...
            val e = if(e1 > e2) e1 else e2  // set end time...
            val x = tuple.getDouble(8)
            val y = tuple.getDouble(9)

            (Flock(s, e, ids, x, y), length)
          }
          .filter(f => f._2 >= mu)
          .map(f => f._1)
          .cache
        var nF = F.count()
        if(debug) showing(F, nF)
        F = pruneFlocks(F, simba).cache()
        nF = F.count()
        if(debug) showing(F, nF)
        logging("D.Filtering", timer, nF, "flocks")

        // Saving current flocks for next iteration...
        F_prime = F
        nF_prime = nF
      }
    }

    // Found flocks...
    timer = System.currentTimeMillis()
    val flocks = F_prime.filter(flock => flock.end - flock.start + 1 == delta).cache()
    val nFlocks = flocks.count()
    if(debug) F_prime.show(truncate = false)
    logging("Found flocks", timer, nFlocks, "flocks")

    // Reporting summary...
    logger.warn("\n\nPFLOCK_ML\t%.1f\t%d\t%d\t%d\n".format(epsilon, mu, delta, nFlocks))

    flocks.as[Flock]
  }

  def pruneFlocks(flocks: Dataset[Flock], simba: SimbaSession): Dataset[Flock] = {
    import simba.implicits._
    import simba.simbaImplicits._

    pruneIDsSubsets(flocks, simba).rdd
      .map(f => (f.ids, f))
      .reduceByKey{ (a,b) =>
        if(a.start < b.start) a else b
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
      .select("start", "end", "ids", "x", "y").as[Flock]
      .groupBy("start", "end", "ids") // Filtering flocks with same IDs...
      .agg(min("x").alias("x"), min("y").alias("y"))
      .repartition(partitions) // Restoring to previous number of partitions...
      .as[Flock]
  }

  private def runFPMax(data: Iterator[String]): Iterator[String] = {
    val transactions = data.toList.map(disk => disk.split(" ").map(new Integer(_)).toList.asJava).asJava
    val algorithm = new AlgoFPMax

    algorithm.runAlgorithm(transactions, 1).getItemsets(1).asScala.map(m => m.asScala.toList.sorted.mkString(" ")).toList.toIterator
  }

  def reorder(list: List[Int], delta: Int): List[Int] = {
    list.grouped(delta).map(binarySearchOrder).reduce(_ union _)
  }

  private def binarySearchOrder(list: List[Int]): List[Int] = {
    if(list.lengthCompare(3) < 0) return list.reverse
    val result = new ListBuffer[Int]()
    val queue = new mutable.Queue[(Int, Int)]()
    var lo = 0
    var hi = list.length - 1

    result += hi
    result += lo
    queue.enqueue((lo, hi))

    while(queue.nonEmpty){
      val pair = queue.dequeue()
      val lo = pair._1
      val hi = pair._2
      if(lo + 1 == hi){
      } else {
        val mi = Math.ceil(lo + (hi - lo) / 2.0).toInt
        result += mi
        queue.enqueue((mi, hi))
        queue.enqueue((lo, mi))
      }
    }

    result.toList.map(i => list(i))
  }

  def showing(flocks: Dataset[Flock], limit: Long): Unit ={
    flocks.orderBy("start", "ids").show(limit.toInt, truncate = false)
    println(s"Number of flocks: ${flocks.count()}\n\n")
  }

  def logging(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    logger.info("%-50s | %6.2fs | %6d %s".format(msg, (System.currentTimeMillis() - timer)/1000.0, n, tag))
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting app...")
    val conf = new Conf(args)
    FlockFinderBenchmark.run(conf)
  }
}

