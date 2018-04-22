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

object FlockFinderBenchmark2 {
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


    /*
    // Running SpatialJoin variant...
    logger.info("")
    logger.warn("method=SpatialJoin,epsilon=%.1f,mu=%d,delta=%d".format(epsilon, mu, delta))
    logger.info("")
    timer = System.currentTimeMillis()
    var flocks: Dataset[Flock] = runSpatialJoin(simba, timestamps, pointset, conf)
    val nFlocks = flocks.count()
    if(printFlocks){ // Reporting final set of flocks...
      flocks.orderBy("start", "ids").show(100, truncate = false)
      logger.warn("\n\nFinal flocks: %d\n".format(nFlocks))
    }
    logging("Running SpatialJoin variant", timer, nFlocks, "flocks")
    */

    // Running MergeLast variant...
    logger.info("")
    logger.warn("method=MergeLast,epsilon=%.1f,mu=%d,delta=%d".format(epsilon, mu, delta))
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

  /* MergeLast variant */
  def runMergeLast(simba: SimbaSession, timestamps: List[Int], pointset: Dataset[ST_Point], conf: Conf): Dataset[Flock] ={
    // Initialize partial result set...
    import simba.implicits._
    import simba.simbaImplicits._

    var F_prime: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nF_prime: Long = 0
    var F: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nF: Long = 0
    var timer: Long = 0
    val epsilon: Double = conf.epsilon()
    val mu: Int = conf.mu()
    val delta: Int = conf.delta()
    val partitions: Int = conf.partitions()
    val debug: Boolean = conf.debug()
    val speed: Double = conf.speed()
    val distanceBetweenTimestamps = speed * delta

    // For each new time instance t_i...
    for(timestamp <- timestamps){
      // Reported location for trajectories in time t_i...
      timer = System.currentTimeMillis()
      val points = pointset
        .filter(datapoint => datapoint.t == timestamp)
        .map{ datapoint =>
          "%d\t%f\t%f".format(datapoint.id, datapoint.x, datapoint.y)
        }
        .rdd
        .cache()
      val nPoints = points.count()
      logging("Reporting locations", timer, nPoints)

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
      logging("Getting maximal disks", timer, nC)

      if(nF_prime != 0) {
        // Distance Join phase with previous potential flocks...
        timer = System.currentTimeMillis()
        C.index(RTreeType, "cRT", Array("x", "y"))
        F_prime.index(RTreeType, "f_primeRT", Array("x", "y"))
        val U = F_prime.distanceJoin(C, Array("x", "y"), Array("x", "y"), distanceBetweenTimestamps)
        val nU = U.count()
        if (debug) U.show(truncate = false)
        logging("Joining", timer, nU)

        // At least mu...
        timer = System.currentTimeMillis()
        val mu = conf.mu()
        var F = U.map{ tuple =>
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
          .filter(flock => flock._2 >= mu)
          .map(_._1)

        F = pruneFlocks(F, partitions, simba).toDS().cache()
        val nF = F.count()
        if(debug) F.show(truncate = false)
        logging("Filtering", timer, nF)
      } else {
        F_prime = C
        nF_prime = nC
      }
    }

    // Found flocks...
    timer = System.currentTimeMillis()
    val flocks = F_prime.filter(flock => flock.end - flock.start + 1 == delta).cache()
    val nFlocks = flocks.count()
    if(debug) F_prime.show(truncate = false)
    logging("Found flocks", timer, nFlocks)

    // Reporting summary...
    logger.warn("\n\nPFLOCK_ML\t%.1f\t%d\t%d\t%d\n".format(epsilon, mu, delta, nFlocks))

    flocks
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
        U = pruneFlocks(U_prime, partitions, simba).toDS().cache()
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

  /* MergeLast variant previous code */
  def runMergeLast2(simba: SimbaSession, timestamps: List[Int], pointset: Dataset[ST_Point], conf: Conf): Dataset[Flock] ={
    // Initialize partial result set...
    import simba.implicits._
    import simba.simbaImplicits._

    var FinalFlocks: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var timer: Long = 0
    val delta: Int = conf.delta()
    val debug: Boolean = conf.debug()
    val speed: Double = conf.speed()
    val distanceBetweenTimestamps = speed * delta

    var F_prime: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nF_prime: Long = 0
    var counter: Int = 1 // Control when to report flocks...
    var U_prime: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS() // Keep potential flocks for the next window...
    var nU_prime: Long = 0


    // For each new time instance t_i in timestamps...
    for(timestamp <- timestamps){
      // Reporting locations...
      timer = System.currentTimeMillis()
      val points = pointset
        .filter(datapoint => datapoint.t == timestamp)
        .map{ datapoint =>
          "%d\t%f\t%f".format(datapoint.id, datapoint.x, datapoint.y)
        }
        .cache()
      val nPoints = points.count()
      logging("Reporting locations", timer, nPoints, "points")

      // Getting maximal disks...
      timer = System.currentTimeMillis()
      val C: Dataset[Flock] = MaximalFinderExpansion
        .run(points.rdd, simba, conf)
        .map{ m =>
          val disk = m.split(";")
          val x = disk(0).toDouble
          val y = disk(1).toDouble
          val ids = disk(2)
          Flock(timestamp, timestamp, ids, x, y)
        }.toDS().cache()
      val nC = C.count()
      logging("Getting maximal disks", timer, nC, "disks")

      if(nF_prime == 0) { // If F' is empty just assign C...
        F_prime = C
        nF_prime = nC
      } else {
        // Join phase...
        timer = System.currentTimeMillis()
        F_prime.index(RTreeType, "f_primeRT", Array("x", "y"))
        C.index(RTreeType, "cRT", Array("x", "y"))
        val mu = conf.mu()
        val F = F_prime.distanceJoin(C, Array("x", "y"), Array("x", "y"), distanceBetweenTimestamps)
          .map { tuple =>
            val ids1 = tuple.getString(1).split(" ").map(_.toLong)
            val ids2 = tuple.getString(5).split(" ").map(_.toLong)
            val intersection = ids1.intersect(ids2)
            val ids = intersection.sorted.mkString(" ") // set flocks ids...
          val x = tuple.getDouble(6) // set flock location...
          val y = tuple.getDouble(7)
            (Flock(timestamp, timestamp, ids, x, y), intersection.length)
          }
          .filter(flock => flock._2 >= mu)
          .map(_._1).as[Flock].cache()
        val nF = F.count()
        F.orderBy("start", "ids").show(100, truncate = false)
        logging("Join phase", timer, nF, "flocks")

        // Filtering phase...
        timer = System.currentTimeMillis()
        F_prime = pruneSubsets(F.union(F_prime), simba).cache()
        nF_prime = F_prime.count()
        F_prime.orderBy("start", "ids").show(100, truncate = false)
        logging("Filtering phase", timer, nF_prime, "flocks")
      }

      if(debug) logger.warn(s"Timestamp=$timestamp Counter=$counter")
      if(counter < delta) {
        counter += 1
      } else { // Reporting flocks...
        val delta = conf.delta()
        /*
        val Flocks = F_prime.filter(f => f.interval.split("").map(_.toInt).reduce(_+_) == delta)
            .map(f => Flock(timestamp, timestamp - delta + 1, f.ids, f.x, f.y))
        if (debug) {
          Flocks.orderBy("start", "ids").show(truncate = false)
          logger.warn(s"Flocks count: ${Flocks.count()}")
        }
        FinalFlocks = FinalFlocks.union(Flocks)

        // Updating F_prime...
        F_prime = F_prime.filter(f => f.interval.split("").map(_.toInt).reduce(_+_) != delta)
          .union(Flocks.map(u => Flock2("", u.ids, u.x, u.y)))
          .cache()
        nF_prime = F_prime.count()
        counter = 1
        */
      }
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

  def reorder(list: List[Int], delta: Int): List[Int] = {
    list.grouped(delta).map(binarySearchOrder).reduce(_ union _)
  }

  def binarySearchOrder(list: List[Int]): List[Int] = {
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

  def pruneFlocks(U: Dataset[Flock], partitions: Int, simba: SimbaSession): RDD[Flock] = {
    import simba.implicits._
    var U_prime = U.mapPartitions{ records =>
      var flocks = new ListBuffer[(Flock, Boolean)]()
      for(record <- records){
        flocks += Tuple2(record, true)
      }
      for(i <- flocks.indices){
        for(j <- flocks.indices){
          if(i != j & flocks(i)._2){
            val ids1 = flocks(i)._1.ids.split(" ").map(_.toLong)
            val ids2 = flocks(j)._1.ids.split(" ").map(_.toLong)
            if(flocks(j)._2 & ids1.forall(ids2.contains)){
              val s1 = flocks(i)._1.start
              val s2 = flocks(j)._1.start
              val e1 = flocks(i)._1.end
              val e2 = flocks(j)._1.end
              if(s1 == s2 & e1 == e2){
                flocks(i) = Tuple2(flocks(i)._1, false)
              }
            }
            if(flocks(i)._2 & ids2.forall(ids1.contains)){
              val s1 = flocks(i)._1.start
              val s2 = flocks(j)._1.start
              val e1 = flocks(i)._1.end
              val e2 = flocks(j)._1.end
              if(s1 == s2 & e1 == e2){
                flocks(j) = Tuple2(flocks(j)._1, false)
              }
            }
          }
        }
      }
      flocks.filter(_._2).map(_._1).toIterator
    }.cache()

    U_prime = U_prime.repartition(1).
      mapPartitions{ records =>
        var flocks = new ListBuffer[(Flock, Boolean)]()
        for(record <- records){
          flocks += Tuple2(record, true)
        }
        for(i <- flocks.indices){
          for(j <- flocks.indices){
            if(i != j & flocks(i)._2){
              val ids1 = flocks(i)._1.ids.split(" ").map(_.toLong)
              val ids2 = flocks(j)._1.ids.split(" ").map(_.toLong)
              if(flocks(j)._2 & ids1.forall(ids2.contains)){
                val s1 = flocks(i)._1.start
                val s2 = flocks(j)._1.start
                val e1 = flocks(i)._1.end
                val e2 = flocks(j)._1.end
                if(s1 == s2 & e1 == e2){
                  flocks(i) = Tuple2(flocks(i)._1, false)
                }
              }
              if(flocks(i)._2 & ids2.forall(ids1.contains)){
                val s1 = flocks(i)._1.start
                val s2 = flocks(j)._1.start
                val e1 = flocks(i)._1.end
                val e2 = flocks(j)._1.end
                if(s1 == s2 & e1 == e2){
                  flocks(j) = Tuple2(flocks(j)._1, false)
                }
              }
            }
          }
        }
        flocks.filter(_._2).map(_._1).toIterator
      }.repartition(partitions).cache()

    U_prime.rdd
  }

  def logging(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    logger.info("%-50s | %6.2fs | %6d %s".format(msg, (System.currentTimeMillis() - timer)/1000.0, n, tag))
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
    val conf = new Conf(args)
    FlockFinderBenchmark2.run(conf)
  }
}

