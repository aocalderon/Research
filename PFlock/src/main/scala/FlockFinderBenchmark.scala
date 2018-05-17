import SPMF.AlgoFPMax
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object FlockFinderBenchmark {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  var conf: Conf = new Conf(Array.empty[String])

  case class ST_Point(id: Long, x: Double, y: Double, t: Int = -1)
  case class Flock(start: Int, end: Int, ids: String, x: Double = 0.0, y: Double = 0.0)
  case class FlockPoints(flockID: Long, pointID: Long)

  private var timer: Long = System.currentTimeMillis()
  private var debug: Boolean = false
  private var print: Boolean = false
  private var precision: Double = 0.001
  private var splitEpsilon: Double = 0.0 //0.7071

  def run(): Unit = {
    // Starting a session...
    val master = conf.master()
    val partitions = conf.partitions()
    val cores = conf.cores()
    val epsilon_min = conf.epsilon()
    val epsilon_max = conf.epsilon_max()
    val epsilon_step = conf.epsilon_step()
    val mu_min = conf.mu()
    val mu_max = conf.mu_max()
    val mu_step = conf.mu_step()
    val delta_min = conf.delta()
    val delta_max = conf.delta_max()
    val delta_step = conf.delta_step()

    val simba = SimbaSession.builder()
      .master(master)
      .appName("FlockFinder")
      .config("simba.index.partitions", partitions)
      .config("spark.cores.max", cores)
      .getOrCreate()
    simba.sparkContext.setLogLevel(conf.logs())
    
    import simba.implicits._
    import simba.simbaImplicits._
    var flocks: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS
    var nFlocks: Long = 0
    logging("Starting session", timer)

    // Setting paramaters...
    timer = System.currentTimeMillis()
    print = conf.print()
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

    for(d <- delta_min to delta_max by delta_step; m <- mu_min to mu_max by mu_step; e <- epsilon_min to epsilon_max by epsilon_step){
/*
      // Running MergeLast v2.0...
      logger.info("=== MergeLast Start ===")
      val startML = System.currentTimeMillis()
      flocks = runMergeLast(pointset, timestamps, e, m, d, simba)
      nFlocks = flocks.count()
      logging("Running MergeLast...", startML, nFlocks, "flocks")
      val timeML = (System.currentTimeMillis() - startML) / 1000.0
      logger.info(s"method=MergeLast,cores=$cores,epsilon=$e,mu=$m,delta=$d,time=$timeML,master=$master")
      // Printing results...
      if(print) printFlocks(flocks, "", simba)
      if(debug) saveFlocks(flocks, s"/tmp/PFLOCK-ML_E${conf.epsilon().toInt}_M${conf.mu()}_D${conf.delta()}.txt", simba)
*/
      // Running SpatialJoin...
      logger.info("=== SpatialJoin Start ===")
      val startSJ = System.currentTimeMillis()
      flocks = runSpatialJoin(pointset, timestamps, e, m, d, simba)
      nFlocks = flocks.count()
      logging("Running SpatialJoin...", startSJ, nFlocks, "flocks")
      val timeSJ = (System.currentTimeMillis() - startSJ) / 1000.0
      logger.info(s"method=SpatialJoin,cores=$cores,epsilon=$e,mu=$m,delta=$d,time=$timeSJ,master=$master")
      // Printing results...
      if(print) printFlocks(flocks, "", simba)
      if(debug) saveFlocks(flocks, s"/tmp/PFLOCK_E${conf.epsilon().toInt}_M${conf.mu()}_D${conf.delta()}.txt", simba)

    }
    // Closing all...
    logger.info("Closing app...")
    simba.close()
  }

  /* MergeLast variant */
  def runMergeLast(pointset: Dataset[ST_Point], timestamps: List[Int], epsilon: Double, mu: Int, delta_prime: Int, simba: SimbaSession): Dataset[Flock] ={
    // Initialize partial result set...
    import simba.implicits._
    import simba.simbaImplicits._

    var FinalFlocks: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nFinalFlocks: Long = 0
    var F_prime: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nF_prime: Long = 0
    val delta: Int = delta_prime - 1
    val distanceBetweenTimestamps: Double = conf.speed() * delta_prime
    val r2 = Math.pow(epsilon / 2.0, 2)
    val maximalDisksCache = collection.mutable.Map[Int, Dataset[Flock]]()

    for(timestamp <- timestamps.slice(0, timestamps.length - delta)) {
      // Getting points at timestamp t ...
      timer = System.currentTimeMillis()
      var C_t = maximalDisksCache.getOrElseUpdate(timestamp, getMaximalDisks(pointset, timestamp, epsilon, mu, simba))
      val nC_t = C_t.count()
      // Getting points at timestamp t + delta ...
      var C_tPlusDelta = maximalDisksCache.getOrElseUpdate(timestamp + delta, getMaximalDisks(pointset, timestamp + delta, epsilon, mu, simba))
      val nC_tPlusDelta = C_tPlusDelta.count()
      logging(s"1.Getting disks", timer, nC_t + nC_tPlusDelta, "disks")

      if(nC_t > 0 && nC_tPlusDelta > 0) {
        // Joining timestamps...
        timer = System.currentTimeMillis()
				C_t = C_t.index(RTreeType, "ctRT", Array("x", "y"))
				C_tPlusDelta = C_tPlusDelta.index(RTreeType, "ctRT", Array("x", "y"))
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
          .index(RTreeType, "f_primeRT", Array("x", "y"))
          .cache()
        nF_prime = F_prime.count()
        logging(s"2.Joining timestams", timer, nF_prime, "candidates")
        // if(debug) F_prime.orderBy("ids").show(nF_prime.toInt, truncate = false)

        // Checking internal timestamps...
        timer = System.currentTimeMillis()
        for (t <- Range(timestamp + 1, timestamp + delta)) {
          val P = getFlockPoints(F_prime, simba).as("c").join(pointset.filter(_.t == t).as("p"), $"c.pointID" === $"p.id", "left")
            .groupBy("flockID")
            .agg(collect_list($"id"), collect_list($"x"), collect_list($"y"))
          val P_prime = P.map { p =>
            val fid = p.getLong(0)
            val ids = p.getList[Long](1).asScala.toList.mkString(" ")
            val Xs = p.getList[Double](2).asScala.toList
            val Ys = p.getList[Double](3).asScala.toList
            val d = getMaximalDistance(Xs zip Ys)

            (ids, Xs, Ys, d, fid, Xs.reduce(_ + _) / Xs.length, Ys.reduce(_ + _) / Ys.length)
          }

          val F1_prime = P_prime.filter(d => d._4 <= epsilon * splitEpsilon)
          val nF1 = F1_prime.count()
          val F2_prime = P_prime.filter(d => d._4 >  epsilon * splitEpsilon)
          val nF2 = F2_prime.count()
          logger.warn(s"F1Count=${nF1},F2Count=${nF2}")
          val F1 = F1_prime.map(f => Flock(timestamp, timestamp + delta, f._1, f._6, f._7))
          //F1 = pruneFlocks(F1, simba).cache
          var F2 = simba.sparkContext.emptyRDD[Flock].toDS()
          
          if(nF2 > 0){
            val points = F2_prime.map(p => (p._1, p._2, p._3))
              .toDF("ids", "xs", "ys")
              .flatMap { p =>
                val ids = p.getString(0).split(" ").map(_.toLong)
                val Xs = p.getList[Double](1).asScala.toList
                val Ys = p.getList[Double](2).asScala.toList
                val points = Xs zip Ys zip ids
                points.map(p => s"${p._2}\t${p._1._1}\t${p._1._2}")
              }
              .distinct
              .rdd
              .cache
              logger.info(s"Running MaximalFinder with ${points.count()} points...")
            val timerMF = System.currentTimeMillis
            F2 = MaximalFinderExpansion.run(points, epsilon, mu, simba, conf)
              .map{ m =>
                val disk = m.split(";")
                val x = disk(0).toDouble
                val y = disk(1).toDouble
                val ids = disk(2)
                Flock(timestamp, timestamp + delta, ids, x, y)
              }
              .toDS()
              .cache
            val timeMF = (System.currentTimeMillis - timerMF) / 1000.0
            logger.info(s"MaximalFinder finishes in ${timeMF}s...")
          }
/***********************************************/
          // Distance Join phase with previous potential flocks...
          timer = System.currentTimeMillis()
          val fDS = F1.union(F2).index(RTreeType, "fdsRT", Array("x", "y"))
          val join = F_prime.distanceJoin(fDS, Array("x", "y"), Array("x", "y"), distanceBetweenTimestamps)
          val nJoin = join.count()
          logging("3.Distance Join phase...", timer, nJoin, "combinations")

          // At least mu...
          timer = System.currentTimeMillis()
          val U_prime = join
            .map { tuple =>
              val ids1 = tuple.getString(7).split(" ").map(_.toLong)
              val ids2 = tuple.getString(2).split(" ").map(_.toLong)
              val u = ids1.intersect(ids2)
              val length = u.length
              val s = tuple.getInt(0) // set the initial time...
              val e = timestamp + delta // set the final time...
              val ids = u.sorted.mkString(" ") // set flocks ids...
              val x = tuple.getDouble(8)
              val y = tuple.getDouble(9)
              (Flock(s, e, ids, x, y), length)
            }
            .filter(flock => flock._2 >= mu)
            .map(_._1).as[Flock]
          val U = pruneFlocks(U_prime, simba).cache()
          val nU = U.count()
          logging("4.Getting candidates...", timer, nU, "candidates")

/***********************************************/
          F_prime = U
          nF_prime = F_prime.count()
          logger.info(s"Timestamp $t checked...")
        }
        val F = F_prime
        val nF = F.count()
        logging("5.Checking internal timestamps", timer, nF, "flocks")

        FinalFlocks = FinalFlocks.union(F)
        nFinalFlocks = FinalFlocks.count()
        C_t.dropIndex()
        C_tPlusDelta.dropIndex()
      }
      maximalDisksCache.remove(timestamp)
    }
    // Reporting summary...
    logger.warn("\n\nPFLOCK_ML\t%.1f\t%d\t%d\t%d\n".format(epsilon, mu, delta + 1, nFinalFlocks))
    
    FinalFlocks
  }

  /* SpatialJoin variant */
  def runSpatialJoin(pointset: Dataset[ST_Point], timestamps: List[Int], epsilon: Double, mu: Int, delta: Int, simba: SimbaSession): Dataset[Flock] ={
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
      logging("Reporting locations...", timer, nCurrentPoints, "points")

      // Set of disks for t_i...
      timer = System.currentTimeMillis()
      val C: Dataset[Flock] = MaximalFinderExpansion
        .run(currentPoints, epsilon, mu, simba, conf)
        .map{ m =>
          val disk = m.split(";")
          val x = disk(0).toDouble
          val y = disk(1).toDouble
          val ids = disk(2)
          Flock(timestamp, timestamp, ids, x, y)
        }.toDS().cache()
      val nC = C.count()
      logging("1.Set of disks for t_i...", timer, nC, "disks")

      var nFlocks: Long = 0
      var nJoin: Long = 0
      if(nF_prime != 0) {
        // Distance Join phase with previous potential flocks...
        timer = System.currentTimeMillis()
        val cDS = C.index(RTreeType, "cRT", Array("x", "y"))
        val fDS = F_prime.index(RTreeType, "f_primeRT", Array("x", "y"))
        val join = fDS.distanceJoin(cDS, Array("x", "y"), Array("x", "y"), distanceBetweenTimestamps)
        nJoin = join.count()
        logging("2.Distance Join phase...", timer, nJoin, "combinations")

        // At least mu...
        timer = System.currentTimeMillis()
        val the_mu = conf.mu()
        val U_prime = join
          .map { tuple =>
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
          .map(_._1).as[Flock]
        U = pruneFlocks(U_prime, simba).cache()
        nU = U.count()
        logging("3.Getting candidates...", timer, nU, "candidates")
      } else {
        U = C
        nU = nC
      }
      // Found flocks...
      timer = System.currentTimeMillis()
      val flocks = U.filter(flock => flock.end - flock.start + 1 == delta).cache()
      nFlocks = flocks.count()
      logging("4.Found flocks...", timer, nFlocks, "flocks")

      // Report flock patterns...
      FinalFlocks = FinalFlocks.union(flocks).cache()
      nFinalFlocks = FinalFlocks.count()

      // Update u.t_start. Shift the time...
      timer = System.currentTimeMillis()
      val F = U.filter(flock => flock.end - flock.start + 1 != delta)
        .union(flocks.map(u => Flock(u.start + 1, u.end, u.ids, u.x, u.y)))
        .cache()
      val nF = F.count()
      logging("5.Updating times...", timer, nF, "flocks")

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
      logging("6.Filter phase...", timer, nF_prime, "flocks")
      if(debug) printFlocks(F_prime, "", simba)
    }
    // Reporting summary...
    logger.warn("\n\nPFLOCK_SJ\t%.1f\t%d\t%d\t%d\n"
      .format(epsilon, mu, delta, nFinalFlocks))

    FinalFlocks
  }

  import org.apache.spark.Partitioner
  class FlockPartitioner(override val numPartitions: Int) extends Partitioner {

    def getPartition(key: Any): Int = {
      return key.asInstanceOf[Int]
    }
  }

  private def computeMaximalDisks2(points: Dataset[List[ST_Point]], epsilon: Double, mu: Int, simba: SimbaSession): Dataset[String] = {
    import simba.implicits._
    import simba.simbaImplicits._

    val r2 = Math.pow(epsilon / 2.0, 2)
    points.flatMap{ p: List[ST_Point] =>
        val pairs = getPairs(p, epsilon)
        val centers = pairs.flatMap(pair => computeCenters(pair, r2))
        val disks = getDisks(p, centers, epsilon)
        filterDisks(disks, mu)
      }
  }

  def filterDisks(input: List[List[Long]], mu: Int): List[String] ={
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

  def getDisks(points: List[ST_Point], centers: List[ST_Point], epsilon: Double): List[List[Long]] ={
    val PointCenter = for{ p <- points; c <- centers } yield (p, c)
    PointCenter.map(d => (d._2, d._1.id, dist( (d._1.x, d._1.y), (d._2.x, d._2.y) ))) // Getting the distance between centers and points...
      .filter(_._3 <= (epsilon / 2.0) + precision) // Filtering out those greater than epsilon / 2...
      .map(d => ( (d._1.x, d._1.y), d._2 )) // Selecting center and point ID...
      .groupBy(_._1) // Grouping by the center...
      .map(_._2.map(_._2)) // Selecting just the list of IDs...
      .toList
  }

  def getPairs(points: List[ST_Point], epsilon: Double): List[(ST_Point, ST_Point)] ={
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

  def computeCenters(pair: (ST_Point, ST_Point), r2: Double): List[ST_Point] = {
    var centerPair = collection.mutable.ListBuffer[ST_Point]()
    val X: Double = pair._1.x - pair._2.x
    val Y: Double = pair._1.y - pair._2.y
    val D2: Double = Math.pow(X, 2) + Math.pow(Y, 2)
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

  def getFlockPoints(flocks: Dataset[Flock], simba: SimbaSession): Dataset[FlockPoints] ={
    import simba.implicits._
    import simba.simbaImplicits._
    flocks.map(f => f.ids.split(" ").map(_.toLong))
      .withColumn("flockID", monotonically_increasing_id())
      .withColumn("pointID", explode($"value"))
      .select("flockID", "pointID")
      .as[FlockPoints]
  }

  def getMaximalDisks(pointset: Dataset[ST_Point], t: Int, epsilon: Double, mu: Int, simba: SimbaSession): Dataset[Flock] ={
    import simba.implicits._
    import simba.simbaImplicits._
    // Getting points at timestamp t ...
    val timer1 = System.currentTimeMillis()
    val points = pointset
      .filter(_.t == t)
      .map{ datapoint =>
        "%d\t%f\t%f".format(datapoint.id, datapoint.x, datapoint.y)
      }
      .rdd
    if(debug) logging(s"Points at t=$t...", timer1, points.count(), "points")

    // Getting maximal disks at timestamp t ...
    val timer2 = System.currentTimeMillis()
    val C: Dataset[Flock] = MaximalFinderExpansion
      .run(points, epsilon, mu, simba, conf)
      .map{ m =>
        val disk = m.split(";")
        val x = disk(0).toDouble
        val y = disk(1).toDouble
        val ids = disk(2)
        Flock(t, t, ids, x, y)
      }.toDS()
    if(debug) logging(s"Maximal disks at t=$t...", timer2, C.count(), "disks")

    C
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

  def pruneFlocks(flocks: Dataset[Flock], simba: SimbaSession): Dataset[Flock] = {
    import simba.implicits._
    import simba.simbaImplicits._

    val U = flocks.rdd
    val partitions = U.getNumPartitions

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

    U_prime.toDS()
  }

  def logging(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    logger.info("%-50s | %6.2fs | %6d %s".format(msg, (System.currentTimeMillis() - timer)/1000.0, n, tag))
  }

  def showFlocks(f: Dataset[Flock]): Unit = {
    val n = f.count()
    f.orderBy("start", "ids").show(n.toInt, truncate = false)
    println(s"Number of flocks: $n")
  }

  def printFlocks(flocks: Dataset[Flock], tag: String = "", simba: SimbaSession): Unit = {
    import simba.implicits._
    
    val n = flocks.count()
    val f = flocks.orderBy("start", "end", "ids")
      .map{ f =>
        "%d, %d, %s\n".format(f.start, f.end, f.ids)
      }.collect.mkString("")
    logger.info(s"\n$tag Number of Flocks: $n\n\n$f\n\n$tag Number of Flocks: $n\n")
  }

  def saveFlocks(flocks: Dataset[Flock], filename: String, simba: SimbaSession): Unit = {
    import simba.implicits._
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
    precision = conf.precision()

    FlockFinderBenchmark.run()
  }
}
