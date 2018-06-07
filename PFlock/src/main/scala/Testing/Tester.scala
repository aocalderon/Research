package Testing

import SPMF.{AlgoFPMax, AlgoLCM, Transactions}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object Tester {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  val epsilon = 30
  val mu = 6
  val precision = 0.001
  val master = "spark://169.235.27.134:7077" //"spark://169.235.27.134:7077"
  val cores = 32
  val partitions = 4

  case class ST_Point(id: Long, x: Double, y: Double, t: Int = -1)
  case class Flock(start: Int, end: Int, ids: String, x: Double = 0.0, y: Double = 0.0)
  case class Disk(ids: String, length: Int, x: Double, y: Double)
  case class FlockPoints(flockID: Long, pointID: Long)
  case class Candidate(id: Long, x: Double, y: Double, items: String)
  case class MaximalPoints(partitionId: Int, maximalId: Long, pointId: Long)

  def main(args: Array[String]): Unit = {
    var timer = System.currentTimeMillis()
    val simba = SimbaSession.builder().master(master)
      .appName("Test")
      .config("simba.index.partitions", s"$partitions")
      .getOrCreate()
    logging("Starting session", timer)
    import simba.implicits._

    // Reading data...
    timer = System.currentTimeMillis()
    val flocks = readingData("/home/acald013/Research/Datasets/Flocks1.txt", simba).cache
    val nFlocks = flocks.count()
    flocks.show(flocks.count().toInt, truncate = false)
    logging("Reading data...", timer, nFlocks, "flocks")

    // Testing pruneFlocks...
    timer = System.currentTimeMillis()
    val F1 = pruneFlocks(flocks, simba: SimbaSession).cache
    val nF1 = F1.count()
    F1.orderBy($"ids").show(nF1.toInt, truncate = false)
    logging("Testing pruneFlocks...", timer, nF1, "flocks")

    // Testing pruneFlocksByExpansions
    timer = System.currentTimeMillis()
    val F2 = pruneFlocksByExpansions(flocks, epsilon, mu, simba: SimbaSession).cache
    val nF2 = F2.count()
    F2.orderBy($"ids").show(nF1.toInt, truncate = false)
    logging("Testing pruneFlocksByExpansions...", timer, nF2, "flocks")

    simba.stop()
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
    candidatesNumPartitions = 3 //nCandidates.toInt / cores
    val candidatesPartitioner: STRPartitioner = new STRPartitioner(candidatesNumPartitions
      , candidatesSampleRate
      , candidatesDimension
      , candidatesTransferThreshold
      , candidatesMaxEntriesPerNode
      , pointCandidate)
    logging("Indexing candidates...", timer, nCandidates, "candidates")

logger.warn(s"Number of partitions: ${candidatesPartitioner.mbrBound.length}")
candidatesPartitioner.mbrBound.foreach(println)

    // Getting expansions...
    timer = System.currentTimeMillis()
    val expandedMBRs = candidatesPartitioner.mbrBound
      .map{ mbr =>
        val mins = new Point( Array(mbr._1.low.coord(0) - epsilon, mbr._1.low.coord(1) - epsilon) )
        val maxs = new Point( Array(mbr._1.high.coord(0) + epsilon, mbr._1.high.coord(1) + epsilon) )
        ( MBR(mins, maxs), mbr._2, 1 )
      }

logger.warn(s"Number of partitions: ${expandedMBRs.length}")
expandedMBRs.foreach(println)

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

candidates2.mapPartitionsWithIndex((i, p) => p.map(m => (m.x, m.y, m.items, i))).
  toDF("x", "y", "ids", "pid").orderBy($"ids").show(100, truncate = false)

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
          val algorithm = new AlgoFPMax
          val maximals = algorithm.runAlgorithm(transactions, 1)

          maximals.getItemsets(mu)
            .asScala
            .map(m => (partitionIndex, m.asScala.toList.sorted.mkString(" ")))
            .toIterator
      }.cache()
    var nMaximals = maximals.map(_._2).distinct().count()
    logging("Finding local maximals...", timer, nMaximals, "local maximals")

maximals.map(m => (m._1, m._2)).toDF("pid", "ids").orderBy($"ids").show(100, truncate = false)

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

maximals2.toDF("A","B","C").orderBy($"A").show(100, truncate = false)

    val prunedFlocks = maximals2.filter(m => m._3).
      map(m => m._1).//distinct().
      map{ m =>
        val arr = m.split(";")
        val ids = arr(0)
        val x   = arr(1).toDouble
        val y   = arr(2).toDouble

        Flock(0, 0, ids, x, y)
      }.as[Flock].cache
    val nPrunedFlocks = prunedFlocks.count()
    logging("Prunning duplicates and subsets...", timer, nPrunedFlocks, "flocks")

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

    bordersX.show()
    bordersY.show()
    prunedFlocks.join(bordersX, "x").show()
    prunedFlocks.join(bordersY, "y").show()

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

  private def readingData(filename: String, simba: SimbaSession): Dataset[Flock] ={
    import simba.implicits._
    import simba.simbaImplicits._

    simba.read.
      option("header", "false").option("sep", ",").
      schema(ScalaReflection.schemaFor[Flock].dataType.asInstanceOf[StructType]).csv(filename).as[Flock].
      map{ f: Flock =>
        val x = BigDecimal(f.x).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
        val y = BigDecimal(f.y).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble

        Flock(f.start, f.end, f.ids, x, y)
      }
  }

  def logging(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    logger.info("%-50s | %6.2fs | %6d %s".format(msg, (System.currentTimeMillis() - timer)/1000.0, n, tag))
  }
}
