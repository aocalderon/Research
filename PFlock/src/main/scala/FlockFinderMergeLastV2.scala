import SPMF.AlgoFPMax
import com.typesafe.config._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object FlockFinderMergeLastV2 {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  var conf: Conf = new Conf(Array.empty[String])

  case class ST_Point(id: Int, x: Double, y: Double, t: Int)
  case class Flock(start: Int, end: Int, ids: String, x: Double = 0.0, y: Double = 0.0)
  case class FlockPoints(flockID: Long, pointID: Long)

  // Starting a session...
  private val params = ConfigFactory.load("flockfinder.conf")
  private val master = params.getString("master")
  private val partitions = params.getString("partitions")
  private val cores = params.getString("cores")

  private val simba = SimbaSession.builder()
    .master(master)
    .appName("FlockFinder")
    .config("simba.index.partitions", partitions)
    .config("spark.cores.max", cores)
    .getOrCreate()
  simba.sparkContext.setLogLevel(conf.logs())
  import simba.implicits._
  import simba.simbaImplicits._
  private var timer: Long = System.currentTimeMillis()
  private var debug: Boolean = false
  var flocks: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS
  var nFlocks: Long = 0
  logging("Starting session", timer)

  def run(): Unit = {
    // Setting paramaters...
    timer = System.currentTimeMillis()
    debug = conf.debug()
    val separator: String = conf.separator()
    val path: String = conf.path()
    val dataset: String = conf.dataset()
    val extension: String = conf.extension()
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

    // Printing results...
    printFlocks(flocks)
    if(debug) saveFlocks(flocks, s"/tmp/PFlock_E${conf.epsilon()}_M${conf.mu()}_D${conf.delta()}.txt")

    // Closing all...
    logger.info("Closing app...")
    simba.close()
  }

  /* MergeLast variant */
  def runMergeLast(pointset: Dataset[ST_Point], timestamps: List[Int]): Unit ={
    // Initialize partial result set...
    import simba.implicits._
    import simba.simbaImplicits._

    var F_prime: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nF_prime: Long = 0
    val delta: Int = conf.delta() - 1
    val mu: Int = conf.mu()
    val distanceBetweenTimestamps: Double = conf.speed() * conf.delta()
    val epsilon: Double = conf.epsilon()

    for(timestamp <- timestamps.slice(0, timestamps.length - delta)) {
      // Getting points at timestamp t ...
      timer = System.currentTimeMillis()
      val C_t = getMaximalDisks(pointset, timestamp)
      val nC_t = C_t.count()
      logging(s"Getting maximal disks: t=$timestamp...", timer, nC_t, "disks")

      // Getting points at timestamp t + delta ...
      timer = System.currentTimeMillis()
      val C_tPlusDelta = getMaximalDisks(pointset, timestamp + delta)
      val nC_tPlusDelta = C_tPlusDelta.count()
      logging(s"Getting maximal disks: t=${timestamp + delta}...", timer, nC_tPlusDelta, "disks")

      // Joining timestamps...
      timer = System.currentTimeMillis()
      F_prime = C_t.distanceJoin(C_tPlusDelta, Array("x", "y"), Array("x", "y"), distanceBetweenTimestamps)
        .map { r =>
          val start = r.getInt(0)
          val end = r.getInt(6)
          val ids1 = r.getString(2).split(" ").map(_.toLong)
          val ids2 = r.getString(7).split(" ").map(_.toLong)
          val ids = ids1.intersect(ids2)
          val len = ids.length
          val x = r.getDouble(3)
          val y = r.getDouble(4)

          (Flock(start, end, ids.sorted.mkString(" "), x, y), len)
        }
        .filter(_._2 >= mu)
        .map(_._1)
        .cache()
      nF_prime = F_prime.count()
      logging(s"Joining timestams: $timestamp vs ${timestamp + delta}...", timer, nF_prime, "candidates")

      // Checking internal timestamps...
      timer = System.currentTimeMillis()
      for (t <- Range(timestamp + 1, timestamp + delta)) {
        F_prime = getFlockPoints(F_prime).as("c").join(pointset.filter(_.t == t).as("p"), $"c.pointID" === $"p.id", "left")
          .groupBy("flockID")
          .agg(collect_list($"id"), collect_list($"x"), collect_list($"y"))
          .map{ p =>
            val ids = p.getList[Long](1).asScala.toList.mkString(" ")
            val Xs  = p.getList[Double](2).asScala.toList
            val Ys  = p.getList[Double](3).asScala.toList
            val d   = getMaximalDistance(Xs zip Ys)

            (Flock(timestamp, timestamp + delta, ids), d)
          }
          .filter(_._2 <= epsilon)
          .map(_._1)
          .cache()
        nF_prime = F_prime.count()
      }
      val F = pruneIDsSubsets(F_prime).cache()
      val nF = F.count()
      logging("Checking internal timestamps...", timer, nF, "flocks")

      if(debug) showFlocks(F)
      flocks = flocks.union(F)
    }
    nFlocks = flocks.count()
  }

  def getMaximalDistance(p: List[(Double, Double)]): Double ={
    val n: Int = p.length
    var max: Double = 0.0
    for(i <- Range(0, n - 1)){
      for(j <- Range(i + 1, n)) {
        val temp = dist(p(i), p(j))
        if(temp > max){
          max = temp
        }
      }
    }
    max
  }

  def printDistanceMatrix(m: Array[Array[Double]]): String ={
    s"\n${m.map(_.map("%.2f".format(_)).mkString("\t")).mkString("\n")}\n"
  }

  def getDistanceMatrix(p: List[(Double, Double)]): Array[Array[Double]] ={
    val n = p.length
    val m = Array.ofDim[Double](n,n)
    for(i <- Range(0, n - 1)){
      for(j <- Range(i + 1, n)) {
        m(i)(j) = dist(p(i), p(j))
      }
    }
    m
  }

  import Math.{pow, sqrt}
  def dist(p1: (Double, Double), p2: (Double, Double)): Double ={
    sqrt(pow(p1._1 - p2._1, 2) + pow(p1._2 - p2._2, 2))
  }

  def getFlockPoints(flocks: Dataset[Flock]): Dataset[FlockPoints] ={
    flocks.map(f => f.ids.split(" ").map(_.toLong))
      .withColumn("flockID", monotonically_increasing_id())
      .withColumn("pointID", explode($"value"))
      .select("flockID", "pointID")
      .as[FlockPoints]
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
        U = pruneSubsets(U_prime).cache()
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

  private def pruneSubsets(flocks: Dataset[Flock]): Dataset[Flock] = {
    import simba.implicits._

    pruneIDsSubsets(flocks).rdd
      .map(f => (f.ids, f))
      .reduceByKey{ (a,b) =>
        if(a.start < b.start){ a } else { b }
      }
      .map(_._2)
      .toDS()
  }

  private def pruneIDsSubsets(flocks: Dataset[Flock]): Dataset[Flock] = {
    import simba.implicits._

    val partitions = flocks.rdd.getNumPartitions
    flocks.map(_.ids)
      .mapPartitions(runFPMax) // Running local...
      .repartition(1)
      .mapPartitions(runFPMax) // Running global...
      .toDF("ids")
      .join(flocks, "ids") // Getting back x and y coordiantes...
      .select("start", "end", "ids", "x", "y")
      .as[Flock]
      .groupBy("start", "end", "ids") // Filtering flocks with same IDs...
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

  def showFlocks(f: Dataset[Flock]): Unit = {
    val n = f.count()
    f.orderBy("start", "ids").show(n.toInt, truncate = false)
    println(s"Number of flocks: $n")
  }

  def printFlocks(flocks: Dataset[Flock]): Unit = {
    val f = flocks.orderBy("start", "end", "ids")
      .map{ f =>
        "%d, %d, %s\n".format(f.start, f.end, f.ids)
      }.collect.mkString("")
    logger.info(s"\nNumber of Flocks: $nFlocks\n\n$f\n\nNumber of Flocks: $nFlocks\n")
  }

  def saveFlocks(flocks: Dataset[Flock], filename: String): Unit = {
    new java.io.PrintWriter(filename) {
      write(
        flocks.map{ f =>
          "%d, %d, %s\n".format(f.start, f.end, f.ids)
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
