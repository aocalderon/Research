import SPMF.{AlgoFPMax, AlgoLCM2, Transactions}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object FlockFinderMergeLast {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  var conf: Conf = new Conf(Array.empty[String])

  case class ST_Point(id: Long, x: Double, y: Double, t: Int = -1)
  case class Flock(start: Int, end: Int, ids: String, x: Double = 0.0, y: Double = 0.0)
  case class Disk(ids: String, length: Int, x: Double, y: Double)
  case class FlockPoints(flockID: Long, pointID: Long)
  case class Candidate(id: Long, x: Double, y: Double, items: String)

  private var timer: Long = System.currentTimeMillis()
  private var debug: Boolean = false
  private var print: Boolean = false
  private var precision: Double = 0.001
  private var splitEpsilon: Double = 0.0

  def run(): Unit = {
    // Starting a session...
    val master = conf.master()
    val partitions = conf.partitions()
    val cores = conf.cores()
    val epsilon_min = conf.epsilon()
    var epsilon_max = conf.epsilon_max()
    if(epsilon_max < epsilon_min){ epsilon_max = epsilon_min }
    val epsilon_step = conf.epsilon_step()
    val mu_min = conf.mu()
    var mu_max = conf.mu_max()
    if(mu_max < mu_min){ mu_max = mu_min }
    val mu_step = conf.mu_step()
    val delta_min = conf.delta()
    var delta_max = conf.delta_max()
    if(delta_max < delta_min){ delta_max = delta_min }
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
    var flocks: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
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
    val point_schema = ScalaReflection.schemaFor[ST_Point].dataType.asInstanceOf[StructType]
    logging("Setting paramaters", timer)

    // Reading data...
    timer = System.currentTimeMillis()
    val filename = "%s%s%s.%s".format(home, path, dataset, extension)
    val pointset = simba.read.
      option("header", "false").
      option("sep", separator).
      schema(point_schema).
      csv(filename).
      as[ST_Point].cache()
    val nPointset = pointset.count()
    logging("Reading data", timer, nPointset, "points")
    
    // Extracting timestamps...
    timer = System.currentTimeMillis()
    val timestamps = pointset.map(datapoint => datapoint.t).distinct.sort("value").collect.toList
    val nTimestamps = timestamps.length
    logging("Extracting timestamps", timer, nTimestamps, "timestamps")

    for(d <- delta_min to delta_max by delta_step; m <- mu_min to mu_max by mu_step; e <- epsilon_min to epsilon_max by epsilon_step){

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
    val distanceBetweenTimestamps: Double = conf.speed() * delta
    val r2 = Math.pow(epsilon / 2.0, 2)
    val maximalDisksCache = collection.mutable.Map[Int, Dataset[Flock]]()

    for(timestamp <- timestamps.slice(0, timestamps.length - delta)) {
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
      logging(s"Reporting locations at t=${timestamp}...", timer, nCurrentPoints, "points")

      // Set of disks for t_i...
      timer = System.currentTimeMillis()
      var C_t: Dataset[Flock] = maximalDisksCache.getOrElseUpdate(timestamp,
        MaximalFinderExpansion.run(currentPoints, epsilon, mu, simba, conf, timestamp)
          .map{ m =>
            val disk = m.split(";")
            val x = disk(0).toDouble
            val y = disk(1).toDouble
            val ids = disk(2)
            Flock(timestamp, timestamp, ids, x, y)
          }.toDS()
      )
      val nC_t = C_t.count()
      logging("1.Set of disks for t_i...", timer, nC_t, "disks")

      // Reported location for trajectories in time t_i+delta...
      timer = System.currentTimeMillis()
      val deltaPoints = pointset
        .filter(datapoint => datapoint.t == timestamp + delta)
        .map{ datapoint =>
          "%d\t%f\t%f".format(datapoint.id, datapoint.x, datapoint.y)
        }
        .rdd
        .cache()
      val nDeltaPoints = deltaPoints.count()
      logging(s"Reporting locations at t=${timestamp+delta}...", timer, nDeltaPoints, "points")

      // Set of disks for t_i+delta...
      timer = System.currentTimeMillis()
      var C_tPlusDelta: Dataset[Flock] = maximalDisksCache.getOrElseUpdate(timestamp + delta , MaximalFinderExpansion.run(deltaPoints, epsilon, mu, simba, conf, timestamp + delta).map{ m =>
            val disk = m.split(";")
            val x = disk(0).toDouble
            val y = disk(1).toDouble
            val ids = disk(2)
            Flock(timestamp, timestamp, ids, x, y)
          }.toDS())
      val nC_tPlusDelta = C_tPlusDelta.count()
      logging("2.Set of disks for t_i+delta...", timer, nC_tPlusDelta, "disks")

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
            val x = r.getDouble(8)
            val y = r.getDouble(9)

            (Flock(start, end, ids.sorted.mkString(" "), x, y), len)
          }
          .filter(_._2 >= mu)
          .map(_._1)
          .index(RTreeType, "f_primeRT", Array("x", "y"))
          .cache()
        nF_prime = F_prime.count()
        logging(s"3.Joining timestams", timer, nF_prime, "candidates")
        if(debug) F_prime.orderBy("ids").show(25, truncate = false)

        // Checking internal timestamps...
        val timerCIT = System.currentTimeMillis()
        val pointset_prime = pointset.filter($"t" > timestamp && $"t" < timestamp + delta)

        // Distance Join phase with previous potential flocks...
        val P = getFlockPoints(F_prime, simba).as("c")
          .join(pointset_prime.as("p"), $"c.pointID" === $"p.id", "left")
          .groupBy("flockID")
          .agg(collect_list($"id"), collect_list($"x"), collect_list($"y"), collect_list($"t"))
        if(debug) logger.warn("Extracting points from raw data...")
        val P_prime = P.map { p =>
          val fid = p.getLong(0)
          val ids = p.getList[Long](1).asScala.toList
          val Xs = p.getList[Double](2).asScala.toList
          val Ys = p.getList[Double](3).asScala.toList
          val ts = p.getList[Int](4).asScala.toList

          val points = ids.zip(Xs).zip(Ys).zip(ts).map(m => ST_Point(m._1._1._1, m._1._1._2, m._1._2, m._2)).sortBy(_.t)

          (fid, points)
        }
        if(debug) logger.warn("Making ST_Points...")
        val F_temp = P_prime.map{ f =>
              val fid = f._1
              val flocks = computeMaximalDisks(f._2, epsilon: Double, mu: Int, simba: SimbaSession)
              (fid, flocks)
          }.
          filter(!_._2.isEmpty).
          flatMap(_._2).
          map(f => Flock(timestamp, timestamp + delta, f.ids, f.x, f.y)).
          cache
        val nF_temp = F_temp.count()
        if(debug){
          logger.warn(s"F_temp count: $nF_temp")
          F_temp.orderBy($"ids").show(25, truncate = false)
        }
        
        //val F =  pruneFlocks(F_temp, simba)
        val F =  pruneFlocksByExpansions(F_temp, epsilon, mu, simba).map(f => Flock(timestamp, timestamp + delta, f.ids, f.x, f.y))
        val nF = F.count()
        logging("Checking internal timestamps", timerCIT, nF, "flocks")

        FinalFlocks = FinalFlocks.union(F)
        nFinalFlocks = FinalFlocks.count()
        C_t.dropIndex()
        C_tPlusDelta.dropIndex()
      }
      maximalDisksCache.remove(timestamp)
    }
    // Reporting summary...
    logger.warn("\n\nPFLOCK_ML\t%.1f\t%d\t%d\t%d\n".format(epsilon, mu, delta + 1, nFinalFlocks))
    if(debug) FinalFlocks.orderBy($"ids").show(25, truncate = false)
    FinalFlocks
  }

  import org.apache.spark.sql.simba.index.{RTree, RTreeType}
  import org.apache.spark.sql.simba.partitioner.STRPartitioner
  import org.apache.spark.sql.simba.spatial.{MBR, Point}
  import org.apache.spark.Partitioner

  class ExpansionPartitioner(partitions: Long) extends Partitioner{
    override def numPartitions: Int = partitions.toInt

    override def getPartition(key: Any): Int = {
      key.asInstanceOf[Int]
    }
  }
  
  def pruneFlocksByExpansions(flocks: Dataset[Flock], epsilon: Double, mu: Int, simba: SimbaSession): Dataset[Flock] = {
    // Indexing candidates...
    var timer = System.currentTimeMillis()
    import simba.implicits._
    import simba.simbaImplicits._
    val debug = false
    val sampleRate = 0.01
    val dimensions = 2

    val candidates = flocks.map(f => Candidate(0, f.x, f.y, f.ids)).cache
    val nCandidates = candidates.count()
    var candidatesNumPartitions: Int = candidates.rdd.getNumPartitions
    if(debug) logger.warn("[Partitions Info]Candidates;Before indexing;%d".format(candidatesNumPartitions))
    val pointCandidate = candidates.map{ candidate =>
        ( new Point(Array(candidate.x, candidate.y)), candidate)
      }
      .rdd
      .cache()
    val candidatesSampleRate: Double = sampleRate
    val candidatesDimension: Int = dimensions
    val candidatesTransferThreshold: Long = 800 * 1024 * 1024
    val candidatesMaxEntriesPerNode: Int = 25
    candidatesNumPartitions = nCandidates.toInt / 100
    val candidatesPartitioner: STRPartitioner = new STRPartitioner(candidatesNumPartitions
      , candidatesSampleRate
      , candidatesDimension
      , candidatesTransferThreshold
      , candidatesMaxEntriesPerNode
      , pointCandidate)
    logging("Indexing candidates...", timer, nCandidates, "candidates")

    // Getting expansions...
    timer = System.currentTimeMillis()
    val expandedMBRs = candidatesPartitioner.mbrBound
      .map{ mbr =>
        val mins = new Point( Array(mbr._1.low.coord(0) - epsilon, mbr._1.low.coord(1) - epsilon) )
        val maxs = new Point( Array(mbr._1.high.coord(0) + epsilon, mbr._1.high.coord(1) + epsilon) )
        ( MBR(mins, maxs), mbr._2, 1 )
      }

    val expandedRTree = RTree(expandedMBRs, candidatesMaxEntriesPerNode)
    candidatesNumPartitions = expandedMBRs.length
    val candidates2 = pointCandidate.flatMap{ candidate =>
      expandedRTree.circleRange(candidate._1, 0.0)
        .map{ mbr => 
          ( mbr._2, candidate._2 )
        }
      }
      .partitionBy(new ExpansionPartitioner(candidatesNumPartitions))
      .map(_._2)
      .cache()
    val nCandidates2 = candidates2.count()

    candidatesNumPartitions = candidates2.getNumPartitions
    logging("Getting expansions...", timer, candidatesNumPartitions, "expansions")

    // Finding local maximals...
    timer = System.currentTimeMillis()
    val maximals = candidates2
      .mapPartitionsWithIndex{ (partitionIndex, partitionCandidates) =>
          val transactions = partitionCandidates
            .map { candidate =>
              candidate.items
              .split(" ")
              .map(new Integer(_))
              .toList.asJava
            }.toList.asJava
          val algorithm = new AlgoLCM2
          val data = new Transactions(transactions)

          val maximals = algorithm.run(data)

          maximals.asScala
            .map(m => (partitionIndex, m.asScala.toList.sorted.mkString(" ")))
            .toIterator
      }.cache()
    var nMaximals = maximals.map(_._2).distinct().count()
    logging("Finding local maximals...", timer, nMaximals, "local maximals")

    // Prunning duplicates and subsets...
    timer = System.currentTimeMillis()
    val EMBRs = expandedMBRs.map{ mbr =>
        mbr._2 -> "%f;%f;%f;%f".format(mbr._1.low.coord(0),mbr._1.low.coord(1),mbr._1.high.coord(0),mbr._1.high.coord(1))
    }.toMap
    val maximals2 = maximals.toDF("pid", "ids").join(flocks, "ids").
      select($"pid", $"ids", $"x", $"y").
      map{ m =>
        val pid = m.getInt(0)
        val MBRCoordinates = EMBRs(pid)
        val ids = m.getString(1)
        val x   = m.getDouble(2)
        val y   = m.getDouble(3)
        val maximal = "%s;%f;%f;%d".format(ids, x, y, pid)
        (maximal, MBRCoordinates)
      }.
      map(m => (m._1, m._2, isNotInExpansionArea(m._1, m._2, epsilon))).cache()

    val maximalFlocks = maximals2.filter(m => m._3).
      map(m => m._1).//distinct().
      map{ m =>
        val arr = m.split(";")
        val ids = arr(0)
        val x   = arr(1).toDouble
        val y   = arr(2).toDouble

        Flock(0, 0, ids, x, y)
      }.as[Flock].cache

    val bordersX = simba.createDataset(candidatesPartitioner.mbrBound.
      flatMap{ mbr =>
        val x1 = mbr._1.low.coord(0)
        val x2 = mbr._1.high.coord(0)

        List(x1, x2)
      }.distinct.sorted.drop(1).dropRight(1)).toDF("x")
    val bordersY = simba.createDataset(candidatesPartitioner.mbrBound.
      flatMap{ mbr =>
        val y1 = mbr._1.low.coord(1)
        val y2 = mbr._1.high.coord(1)

        List(y1, y2)
      }.distinct.sorted.drop(1).dropRight(1)).toDF("y")

    val duplicateX = bordersX.
      join(right = maximalFlocks, usingColumns = Seq("x"), joinType = "inner").
      select($"start", $"end", $"ids", $"x", $"y").as[Flock].cache
    val duplicateY = bordersY.
      join(right = maximalFlocks, usingColumns = Seq("y"), joinType = "inner").
      select($"start", $"end", $"ids", $"x", $"y").as[Flock].cache
    val duplicates = duplicateX.union(duplicateY).cache
    val prunedFlocks = maximalFlocks.except(duplicates).union(duplicateX.distinct).union(duplicateY.distinct).cache
    val nPrunedFlocks = prunedFlocks.count()

    logging("Prunning duplicates and subsets...", timer, nPrunedFlocks, "flocks")

    prunedFlocks
  }

  def isNotInExpansionArea(maximalString: String, coordinatesString: String, epsilon: Double): Boolean = {
    val maximal = maximalString.split(";")
    val x = maximal(1).toDouble
    val y = maximal(2).toDouble
    val coordinates = coordinatesString.split(";")
    val min_x = coordinates(0).toDouble
    val min_y = coordinates(1).toDouble
    val max_x = coordinates(2).toDouble
    val max_y = coordinates(3).toDouble

    x <= (max_x - epsilon) &&
      x >= (min_x + epsilon) &&
      y <= (max_y - epsilon) &&
      y >= (min_y + epsilon)
  }

  import org.apache.spark.Partitioner
  class FlockPartitioner(override val numPartitions: Int) extends Partitioner {

    def getPartition(key: Any): Int = {
      return key.asInstanceOf[Int]
    }
  }
  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def computeMaximalDisks(points: List[ST_Point], epsilon: Double, mu: Int, simba: SimbaSession): List[Flock] = {
    val flockList = ListBuffer[List[Flock]]()
    val timestamps = points.map(_.t).distinct
    val r2 = Math.pow(epsilon / 2.0, 2)

    for(timestamp <- timestamps){
      val p = points.filter(_.t == timestamp)
      val pairs = getPairs(p, epsilon)
      val centers = pairs.flatMap(pair => computeCenters(pair, r2))
      var disks = getDisks(p, centers, epsilon)
      disks = filterDisks(disks, mu)

      flockList += disks.map(d => Flock(timestamp, timestamp, d.ids, d.x, d.y))
    }

    var f = flockList.head
    for(f_prime <- flockList.tail){
      f = f.cross(f_prime).map { f => 
        val start = f._1.start
        val end = f._2.end
        val ids1 = f._1.ids.split(" ").map(_.toLong)
        val ids2 = f._2.ids.split(" ").map(_.toLong)
        val ids = ids1.intersect(ids2)
        val len = ids.length
        val x = f._2.x
        val y = f._2.y

        (Flock(start, end, ids.sorted.mkString(" "), x, y), len)
      }.
      toList.
      filter(_._2 >= mu).
      map(_._1)

    }

    f
  }

  def filterDisks(candidates: List[Disk], mu: Int): List[Disk] ={
    var valids = new collection.mutable.ListBuffer[(Disk, Boolean)]()
    for( candidate <- candidates.filter(_.length >= mu) ){ valids += Tuple2(candidate, true) }
    for(i <- valids.indices){
      for(j <- valids.indices){
        if(i != j & valids(i)._2){
          val ids1 = valids(i)._1.ids.split(" ")
          val ids2 = valids(j)._1.ids.split(" ")
          if(valids(j)._2 & ids1.forall(ids2.contains)){
            valids(i) = Tuple2(valids(i)._1, false)
          }
          if(valids(i)._2 & ids2.forall(ids1.contains)){
            valids(j) = Tuple2(valids(j)._1, false)
          }
        }
      }
    }

    valids.filter(_._2).map(_._1).toList
  }

  def getDisks(points: List[ST_Point], centers: List[ST_Point], epsilon: Double): List[Disk] ={
    val t = points.map(_.t).head
    val PointCenter = for{ p <- points; c <- centers } yield (p, c)
    PointCenter.map(d => (d._2, d._1.id, dist( (d._1.x, d._1.y), (d._2.x, d._2.y) ))) // Getting the distance between centers and points...
      .filter(_._3 <= (epsilon / 2.0) + precision) // Filtering out those greater than epsilon / 2...
      .map(d => ( (d._1.x, d._1.y), d._2 )) // Selecting center and point ID...
      .groupBy(_._1) // Grouping by the center...
      .map { disk => // Selecting just the list of IDs...
        val center = disk._1
        val ids = disk._2.map(_._2)
        val len = ids.length

        Disk(ids.mkString(" "), len, center._1, center._2)
      } 
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
      .cache
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

    FlockFinderMergeLast.run()
  }
}
