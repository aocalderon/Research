import SPMF.{AlgoFPMax, AlgoLCM, Transactions}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.{RTree, RTreeType}
import org.apache.spark.sql.simba.partitioner.STRPartitioner
import org.apache.spark.sql.simba.spatial.{MBR, Point}
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object MaximalFinderExpansion {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val precision: Double = 0.001
  private val dimensions: Int = 2
  private val sampleRate: Double = 0.01
  private var phd_home: String = ""
  private var nPoints: Long = 0

  case class SP_Point(id: Long, x: Double, y: Double)
  case class P1(id1: Long, x1: Double, y1: Double)
  case class P2(id2: Long, x2: Double, y2: Double)
  case class Center(id: Long, x: Double, y: Double)
  case class Pair(id1: Long, x1: Double, y1: Double, id2: Long, x2: Double, y2: Double)
  case class Candidate(id: Long, x: Double, y: Double, items: String)
  case class Maximal(id: Long, x: Double, y: Double, items: String)
  case class CandidatePoints(cid: Long, pid: Long)
  case class MaximalPoints(partitionId: Int, maximalId: Long, pointId: Long)
  case class MaximalMBR(maximal: Maximal, mbr: MBR)
  case class BBox(minx: Double, miny: Double, maxx: Double, maxy: Double)

  def run(pointsRDD: RDD[String]
      , epsilon: Double
      , mu : Int  
      , simba: SimbaSession
      , conf: Conf
      , timestamp: Int = -1): RDD[String] = {
    // A.Setting variables...
    val separator = conf.separator()
    val debug = conf.debug()
    var maximals3: RDD[String] = simba.sparkContext.emptyRDD
    val ExpansionSize = conf.expansion_size()
    val dataset = conf.dataset()
    val delta = conf.delta()

    import simba.implicits._
    import simba.simbaImplicits._
    logger.info("Setting mu=%d,epsilon=%.1f,cores=%d,timestamp=%d,dataset=%s"
      .format(mu, epsilon, conf.cores(), timestamp, dataset))
    if(pointsRDD.isEmpty()) return maximals3
    val startTime = System.currentTimeMillis()
    // B.Indexing points...
    var timer = System.currentTimeMillis()
    var pointsNumPartitions = pointsRDD.getNumPartitions
    if(debug) logger.info("[Partitions Info]Points;Before indexing;%d".format(pointsNumPartitions))
    val p1 = pointsRDD.map(_.split(separator)).
      map(p => P1(p(0).trim.toLong,p(1).trim.toDouble,p(2).trim.toDouble)).
      toDS().
      index(RTreeType,"p1RT",Array("x1","y1")).
      cache()
    val p2 = pointsRDD.map(_.split(separator)).
      map(p => P2(p(0).trim.toLong,p(1).trim.toDouble,p(2).trim.toDouble)).
      toDS().
      index(RTreeType,"p2RT",Array("x2","y2")).
      cache()
    val points = pointsRDD.map(_.split(separator)).
      map(p => SP_Point(p(0).trim.toLong,p(1).trim.toDouble,p(2).trim.toDouble)).
      toDS().
      index(RTreeType,"pointsRT",Array("x","y")).
      cache()
    nPoints = points.count()
    pointsNumPartitions = points.rdd.getNumPartitions
    if(debug) logger.info("[Partitions Info]Points;After indexing;%d".format(pointsNumPartitions))
    logger.info("A.Indexing points... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer)/1000.0, nPoints))
    // C.Getting pairs...
    timer = System.currentTimeMillis()
    val pairs = p1.distanceJoin(p2, Array("x1", "y1"), Array("x2", "y2"), epsilon + precision)
      .as[Pair]
      .filter(pair => pair.id1 < pair.id2)
      .rdd
      .cache()
    val nPairs = pairs.count()
    logger.info("B.Getting pairs... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer)/1000.0, nPairs))
    // D.Computing centers...
    timer = System.currentTimeMillis()
    val centerPairs = findCenters(pairs, epsilon)
      .filter( pair => pair.id1 != -1 )
      .toDS()
      .as[Pair]
      .cache()
    val leftCenters = centerPairs.select("x1","y1")
    val rightCenters = centerPairs.select("x2","y2")
    val centersRDD = leftCenters.union(rightCenters)
      .toDF("x", "y")
      .withColumn("id", monotonically_increasing_id())
      .as[SP_Point]
      .rdd
      .repartition(conf.cores())
      .cache()
    val nCenters = centersRDD.count()
    logger.info("C.Computing centers... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer)/1000.0, nCenters))
    // E.Indexing centers...
    timer = System.currentTimeMillis()
    var centersNumPartitions: Int = centersRDD.getNumPartitions
    if(debug) logger.info("[Partitions Info]Centers;Before indexing;%d".format(centersNumPartitions))
    val centers = centersRDD.toDS.index(RTreeType, "centersRT", Array("x", "y")).cache()
    centersNumPartitions = centers.rdd.getNumPartitions
    if(debug) logger.info("[Partitions Info]Centers;After indexing;%d".format(centersNumPartitions))
    logger.info("D.Indexing centers... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer)/1000.0, nCenters))
    // F.Getting disks...
    timer = System.currentTimeMillis()
    val disks = centers
      .distanceJoin(p1, Array("x", "y"), Array("x1", "y1"), (epsilon.toFloat / 2.0) + precision)
      .groupBy("id", "x", "y")
      .agg(collect_list("id1").alias("ids"))
      .cache()
    val nDisks = disks.count()
    logger.info("E.Getting disks... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer)/1000.0, nDisks))
    // G.Filtering less-than-mu disks...
    timer = System.currentTimeMillis()
    val filteredDisks = disks
      .filter(row => row.getList[Long](3).asScala.toList.distinct.length >= mu)
      .rdd
      .cache()
    val nFilteredDisks = filteredDisks.count()
    logger.info("F.Filtering less-than-mu disks... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer)/1000.0, nFilteredDisks))
    // H.Prunning duplicate candidates...
    timer = System.currentTimeMillis()
    val candidatePoints = filteredDisks
      .map{ c =>
        ( c.getLong(0), c.getList[Long](3).asScala.toList.distinct.sorted )
      }
      .toDF("cid", "items")
      .withColumn("pid", explode($"items"))
      .select("cid","pid")
      .as[CandidatePoints]
      .cache()
    val candidates = candidatePoints
      .join(points, candidatePoints.col("pid") === points.col("id"))
      .groupBy($"cid").agg(min($"x"), min($"y"), max($"x"), max($"y"), collect_list("pid"))
      .map{ c => 
        Candidate( 0
          , (c.getDouble(1) + c.getDouble(3)) / 2.0
          , (c.getDouble(2) + c.getDouble(4)) / 2.0
          , c.getList[Long](5).asScala.toList.distinct.sorted.mkString(" ") 
        )
      }
      .distinct()
      .cache()
    val nCandidates = candidates.count()
    //candidates.show(nCandidates.toInt, truncate = false)
    logger.info("G.Prunning duplicate candidates... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer)/1000.0, nCandidates))
    // I.Indexing candidates... 
    if(nCandidates > 0){
      var candidatesNumPartitions: Int = candidates.rdd.getNumPartitions
      logger.info("[Partitions Info]Candidates;Before indexing;%d".format(candidatesNumPartitions))
      val pointCandidate = candidates.map{ candidate =>
          ( new Point(Array(candidate.x, candidate.y)), candidate)
        }
        .rdd
        .cache()
      val candidatesSampleRate: Double = sampleRate
      val candidatesDimension: Int = dimensions
      val candidatesTransferThreshold: Long = 800 * 1024 * 1024
      val candidatesMaxEntriesPerNode: Int = 25
      candidatesNumPartitions = ( nCandidates.toInt / ExpansionSize ).toInt
      val candidatesPartitioner: STRPartitioner = new STRPartitioner(candidatesNumPartitions
        , candidatesSampleRate
        , candidatesDimension
        , candidatesTransferThreshold
        , candidatesMaxEntriesPerNode
        , pointCandidate)
      logger.info("H.Indexing candidates... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer)/1000.0, nCandidates))
      // J.Getting expansions...
      timer = System.currentTimeMillis()
      val expandedMBRs = candidatesPartitioner.mbrBound
        .map{ mbr =>
          val mins = new Point( Array(mbr._1.low.coord(0) - epsilon
            , mbr._1.low.coord(1) - epsilon) )
          val maxs = new Point( Array(mbr._1.high.coord(0) + epsilon
            , mbr._1.high.coord(1) + epsilon) )
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
      logger.info("[Partitions Info]Candidates;After indexing;%d".format(candidatesNumPartitions))
      logger.info("I.Getting expansions... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer)/1000.0, nCandidates2))

      ////////////////////////////////////////////////////////////
      if(debug){
        val datasets = candidates2.mapPartitionsWithIndex{ (partitionIndex, partitionCandidates) =>
          partitionCandidates.map{ m =>
            val ids = m.items.split(" ").map(_.toLong).distinct.mkString(" ")

            s"$partitionIndex,$ids"
          }
        }
        new java.io.PrintWriter(s"/tmp/Datasets_${dataset}_${epsilon}_${mu}_${delta}_${timestamp}.txt") {
          val w = datasets.collect().map(line => s"$line\n").mkString(" ")
          write(w)
          close()
        }
      }
      ////////////////////////////////////////////////////////////

      // K.Finding maximal disks...
      timer = System.currentTimeMillis()
      val method = conf.method()
      val maximals = candidates2
        .mapPartitionsWithIndex{ (partitionIndex, partitionCandidates) =>
          var maximalsIterator: Iterator[(Int, List[Long])] = null
          if(method == "fpmax"){
            val transactions = partitionCandidates
              .map { candidate =>
                candidate.items
                .split(" ")
                .map(new Integer(_))
                .toList.asJava
              }.toList.asJava
            val algorithm = new AlgoFPMax
            val maximals = algorithm.runAlgorithm(transactions, 1)
            maximalsIterator = maximals.getItemsets(mu)
              .asScala
              .map(m => (partitionIndex, m.asScala.toList.map(_.toLong).sorted))
              .toIterator
          } else {
            val transactions = partitionCandidates
              .map { candidate =>
                candidate.items
                .split(" ")
                .map(new Integer(_))
                .toList.asJava
              }.toSet.asJava
            val LCM = new AlgoLCM
            val data = new Transactions(transactions)
            val maximals = LCM.runAlgorithm(1, data)
            maximalsIterator = maximals.getItemsets(mu)
              .asScala
              .map(m => (partitionIndex, m.asScala.toList.map(_.toLong).sorted))
              .toIterator
          }
          maximalsIterator
        }
        .cache()
      var nMaximals = maximals.map(_._2.mkString(" ")).distinct().count()
      logger.info("J.Finding maximal disks... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer)/1000.0, nMaximals))
      // L.Prunning duplicates and subsets...
      timer = System.currentTimeMillis()
      val EMBRs = expandedMBRs.map{ mbr =>
          mbr._2 -> "%f;%f;%f;%f".format(mbr._1.low.coord(0),mbr._1.low.coord(1),mbr._1.high.coord(0),mbr._1.high.coord(1))
        }
        .toMap
      val maximalPoints = maximals
        .toDF("partitionId", "pointsId")
        .withColumn("maximalId", monotonically_increasing_id())
        .withColumn("pointId", explode($"pointsId"))
        .select("partitionId", "maximalId", "pointId")
        .as[MaximalPoints]
        .cache()
      val maximals2 = maximalPoints
        .join(points, maximalPoints.col("pointId") === points.col("id"))
        .groupBy($"partitionId", $"maximalId").agg(min($"x"), min($"y"), max($"x"), max($"y"), collect_list("pointId"))
        .map{ m => 
          val MBRCoordinates = EMBRs(m.getInt(0))
          val x = (m.getDouble(2) + m.getDouble(4)) / 2.0
          val y = (m.getDouble(3) + m.getDouble(5)) / 2.0
          val pids = m.getList[Long](6).asScala.toList.distinct.sorted.mkString(" ")
          val maximal = "%f;%f;%s".format(x, y, pids)
          (maximal, MBRCoordinates)
        }
        .map(m => (m._1, m._2, isNotInExpansionArea(m._1, m._2, epsilon)))
        .cache()
      maximals3 = maximals2
        .filter(m => m._3)
        .map(m => m._1)   
        .distinct()
        .rdd
      nMaximals = maximals3.count()
      logger.info("K.Prunning duplicates and subsets... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer)/1000.0, nMaximals))
      val endTime = System.currentTimeMillis()
      val totalTime = (endTime - startTime)/1000.0
      // Printing info summary ...
      logger.info("%12s,%8s,%6s,%6s,%3s,%7s,%8s,%10s,%13s,%11s,%3s".
        format("Dataset", "# Points","Eps", "Cores", "Mu", "Time",
          "# Pairs", "# Disks", "# Candidates", "# Maximals", "t"
        )
      )
      logger.info("%12s,%8d,%6.1f,%6d,%3d,%7.2f,%8d,%10d,%13d,%11d,%3d".
        format(conf.dataset(), nPoints, epsilon, conf.cores(), mu, totalTime, 
          nPairs, nDisks, nCandidates, nMaximals, timestamp
        )
      )
    }
    // Dropping indices...
    timer = System.currentTimeMillis()
    p1.dropIndex()
    p2.dropIndex()
    points.dropIndex()
    centers.dropIndex()
    logger.info("Dropping indices...[%.3fs]".format((System.currentTimeMillis() - timer)/1000.0))
    
    maximals3
  }

  import org.apache.spark.Partitioner
  class ExpansionPartitioner(partitions: Long) extends Partitioner{
    override def numPartitions: Int = partitions.toInt

    override def getPartition(key: Any): Int = {
      key.asInstanceOf[Int]
    }
  }

  def findCenters(pairs: RDD[Pair], epsilon: Double): RDD[Pair] = {
    val r2: Double = math.pow(epsilon / 2, 2)
    val centerPairs = pairs
      .map { (pair: Pair) =>
        calculateCenterCoordinates(pair, r2)
      }
    centerPairs
  }

  def calculateCenterCoordinates(pair: Pair, r2: Double): Pair = {
    var centerPair = Pair(-1, 0, 0, 0, 0, 0) //To be filtered in case of duplicates...
    val X: Double = pair.x1 - pair.x2
    val Y: Double = pair.y1 - pair.y2
    val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
    if (D2 != 0.0){
      val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
      val h1: Double = ((X + Y * root) / 2) + pair.x2
      val k1: Double = ((Y - X * root) / 2) + pair.y2
      val h2: Double = ((X - Y * root) / 2) + pair.x2
      val k2: Double = ((Y + X * root) / 2) + pair.y2
      centerPair = Pair(pair.id1, h1, k1, pair.id2, h2, k2)
    }
    centerPair
  }

  def isNotInExpansionArea(maximalString: String, coordinatesString: String, epsilon: Double): Boolean = {
    val maximal = maximalString.split(";")
    val x = maximal(0).toDouble
    val y = maximal(1).toDouble
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

  def isInside(x: Double, y: Double, bbox: BBox, epsilon: Double): Boolean = {
    x < (bbox.maxx - epsilon) &&
      x > (bbox.minx + epsilon) &&
      y < (bbox.maxy - epsilon) &&
      y > (bbox.miny + epsilon)
  }

  def saveStringArray(array: Array[String], tag: String, conf: Conf): Unit = {
    val path = s"$phd_home${conf.valpath()}"
    val filename = s"${conf.dataset()}_E${conf.epsilon()}_M${conf.mu()}_C${conf.cores()}"
    new java.io.PrintWriter("%s%s_%s_%d.txt".format(path, filename, tag, System.currentTimeMillis)) {
      write(array.mkString("\n"))
      close()
    }
  }

  def saveStringArrayWithoutTimeMillis(array: Array[String], tag: String, conf: Conf): Unit = {
    val path = s"$phd_home${conf.valpath()}"
    val filename = s"${conf.dataset()}_E${conf.epsilon()}_M${conf.mu()}_C${conf.cores()}"
    new java.io.PrintWriter("%s%s_%s.txt".format(path, filename, tag)) {
      write(array.mkString("\n"))
      close()
    }
  }

  def toWKT(coordinatesString: String): String = {
    val coordinates = coordinatesString.split(";")
    val min_x = coordinates(0).toDouble
    val min_y = coordinates(1).toDouble
    val max_x = coordinates(2).toDouble
    val max_y = coordinates(3).toDouble
    
    toWKT(min_x, min_y, max_x, max_y)
  }

  def toWKT(minx: Double, miny: Double, maxx: Double, maxy: Double): String = {
    "POLYGON (( %f %f, %f %f, %f %f, %f %f, %f %f ))".
      format(minx,maxy,maxx,maxy,maxx,miny,minx,miny,minx,maxy)
  }
  
  def mbr2wkt(mbr: MBR): String = toWKT(mbr.low.coord(0),mbr.low.coord(1),mbr.high.coord(0),mbr.high.coord(1))

  def main(args: Array[String]): Unit = {
    // Reading arguments from command line...
    val conf = new Conf(args)
    val master = conf.master()
    // Starting session...
    var timer = System.currentTimeMillis()
    val simba = SimbaSession.builder()
      .master(master)
      .appName("MaximalFinderExpansion")
      .config("simba.index.partitions",conf.partitions().toString)
      .config("spark.cores.max",conf.cores().toString)
      .getOrCreate()
    import simba.implicits._
    logger.info("Starting session... [%.3fs]".format((System.currentTimeMillis() - timer)/1000.0))
    // Reading...
    timer = System.currentTimeMillis()
    phd_home = scala.util.Properties.envOrElse(conf.home(), "/home/acald013/Research/")
    val filename = "%s%s%s.%s".format(phd_home, conf.path(), conf.dataset(), conf.extension())
    val points = simba.sparkContext.
      textFile(filename).
      cache()
    nPoints = points.count()
    logger.info("Reading dataset... [%.3fs] [%d points]".format((System.currentTimeMillis() - timer)/1000.0, nPoints))
    // Running MaximalFinder...
    logger.info("Lauching MaximalFinder at %s...".format(DateTime.now.toLocalTime.toString))
    val start = System.currentTimeMillis()
    val epsilon = conf.epsilon()
    val mu = conf.mu()
    val disks = MaximalFinderExpansion.run(points, epsilon, mu, simba, conf)
    if(conf.print()){
      logger.info("Showing final set of maximal disks...\n\n")
      disks.toDF().show(disks.count().toInt, truncate = false)
    }
    val end = System.currentTimeMillis()
    logger.info("Finishing MaximalFinder at %s...".format(DateTime.now.toLocalTime.toString))
    logger.info("Total time for MaximalFinder: %.3fms...".format((end - start)/1000.0))
    // Closing session...
    timer = System.currentTimeMillis()
    simba.close
    logger.info("Closing session... [%.3fs]".format((System.currentTimeMillis() - timer)/1000.0))
  }
}
