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

  case class ST_Point(id: Long, x: Double, y: Double, t: Int = -1)
  case class Flock(start: Int, end: Int, ids: String, x: Double = 0.0, y: Double = 0.0)
  case class FlockPoints(flockID: Long, pointID: Long)

  // Starting a session...
  private val params = ConfigFactory.parseFile(new java.io.File("/tmp/flockfinder.conf"))
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
  private var r2: Double = 0.0
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
    if(debug) saveFlocks(flocks, s"/tmp/PFLOCK_E${conf.epsilon().toInt}_M${conf.mu()}_D${conf.delta()}.txt")

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

      if(nC_t > 0 && nC_tPlusDelta > 0) {
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
          val P = getFlockPoints(F_prime).as("c").join(pointset.filter(_.t == t).as("p"), $"c.pointID" === $"p.id", "left")
            .groupBy("flockID")
            .agg(collect_list($"id"), collect_list($"x"), collect_list($"y"))
          val P_prime = P.map { p =>
            val ids = p.getList[Long](1).asScala.toList.mkString(" ")
            val Xs = p.getList[Double](2).asScala.toList
            val Ys = p.getList[Double](3).asScala.toList
            val d = getMaximalDistance(Xs zip Ys)

            (ids, Xs, Ys, d)
          }
          val F1 = P_prime.filter(_._4 <= epsilon * 0.75)
            .map(f => Flock(timestamp, timestamp + delta, f._1))
            .cache()
          val F2 = P_prime.filter(_._4 > epsilon * 0.75)
            .flatMap(f => computeMaximalDisks(f._1, f._2, f._3))
            .map(ids => Flock(timestamp, timestamp + delta, ids))
            .cache()
          F_prime = F1.union(F2)
          //showFlocks(F_prime)
          nF_prime = F_prime.count()
        }
        //showFlocks(F_prime)
        val F = pruneIDsSubsets(F_prime).cache()
        //showFlocks(F)
        val nF = F.count()
        logging("Checking internal timestamps...", timer, nF, "flocks")

        flocks = flocks.union(F)
      }
    }
    nFlocks = flocks.count()
  }

  private def computeMaximalDisks(ids: String, Xs: List[Double], Ys: List[Double]): List[String] = {
    val points = ids.split(" ").map(_.toLong).zip(Xs.zip(Ys)).map(p => ST_Point(p._1, p._2._1, p._2._2)).toList
    val pairs = getPairs(points)
    r2 = Math.pow(conf.epsilon() / 2.0, 2)
    val centers = pairs.flatMap(computeCenters)
    val disks = getDisks(points, centers)
    filterDisks(disks)
  }

  def filterDisks(input: List[List[Long]]): List[String] ={
    val mu = conf.mu()
    var ids = new collection.mutable.ListBuffer[(List[Long], Boolean)]()
    for( disk <- input.filter(_.lengthCompare(mu) >= 0) ){ ids += Tuple2(disk, true) }
    for(i <- ids.indices){
      for(j <- ids.indices){
        if(i != j & ids(i)._2){
          val ids1 = ids(i)._1
          val ids2 = ids(j)._1
          if(ids(j)._2 & ids1.forall(ids2.contains)){
            ids(i) = Tuple2(ids(i)._1, false)
          }
          if(ids(i)._2 & ids2.forall(ids1.contains)){
            ids(j) = Tuple2(ids(j)._1, false)
          }
        }
      }
    }

    ids.filter(_._2).map(_._1.sorted.mkString(" ")).toList
  }

  def getDisks(points: List[ST_Point], centers: List[ST_Point]): List[List[Long]] ={
    val epsilon = conf.epsilon()
    val precision = conf.precision()
    val PointCenter = for{ p <- points; c <- centers } yield (p, c)
    PointCenter.map(d => (d._2, d._1.id, dist( (d._1.x, d._1.y), (d._2.x, d._2.y) ))) // Getting the distance between centers and points...
      .filter(_._3 <= (epsilon / 2.0) + precision) // Filtering out those greater than epsilon / 2...
      .map(d => ( (d._1.x, d._1.y), d._2 )) // Selecting center and point ID...
      .groupBy(_._1) // Grouping by the center...
      .map(_._2.map(_._2)) // Selecting just the list of IDs...
      .toList
  }

  def getPairs(points: List[ST_Point]): List[(ST_Point, ST_Point)] ={
    val epsilon = conf.epsilon()
    val precision = conf.precision()
    val n = points.length
    var pairs = collection.mutable.ListBuffer[(ST_Point, ST_Point)]()
    for(i <- Range(0, n - 1)){
      for(j <- Range(i + 1, n)) {
        val d = dist((points(i).x, points(i).y), (points(j).x, points(j).y))
        if(d <= epsilon + precision){
          pairs += Tuple2(points(i), points(j))
        }
      }
    }
    pairs.toList
  }

  import Math.{pow, sqrt}
  def dist(p1: (Double, Double), p2: (Double, Double)): Double ={
    sqrt(pow(p1._1 - p2._1, 2) + pow(p1._2 - p2._2, 2))
  }

  def computeCenters(pair: (ST_Point, ST_Point)): List[ST_Point] = {
    var centerPair = collection.mutable.ListBuffer[ST_Point]()
    val X: Double = pair._1.x - pair._2.x
    val Y: Double = pair._1.y - pair._2.y
    val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
    if (D2 != 0.0){
      val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
      val h1: Double = ((X + Y * root) / 2) + pair._2.x
      val k1: Double = ((Y - X * root) / 2) + pair._2.y
      val h2: Double = ((X - Y * root) / 2) + pair._2.x
      val k2: Double = ((Y + X * root) / 2) + pair._2.y
      centerPair += ST_Point(pair._1.id, h1, k1)
      centerPair += ST_Point(pair._2.id, h2, k2)
    }
    centerPair.toList
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
        flocks.orderBy("start","end","ids").map{ f =>
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
