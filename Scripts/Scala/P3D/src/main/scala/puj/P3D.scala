package puj

import edu.ucr.dblab.pflock.sedona.quadtree.{
  QuadRectangle,
  StandardQuadTree,
  Quadtree
}

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.GeometryItemDistance

import scala.collection.JavaConverters._
import scala.util.Random

object P3D extends Logging {

  def main(args: Array[String]): Unit = {
    logger.info("Starting P3D application")
    implicit val params = new Params(args)
    implicit val G = new GeometryFactory(
      new PrecisionModel(1.0 / params.tolerance())
    )

    implicit val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("P3D")
      .getOrCreate()
    import spark.implicits._
    logger.info("SparkSession created")

    val pointsRDD = spark.read
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(params.input())
      .rdd
      .map { row =>
        val oid = row.getString(0).toInt
        val lon = row.getString(1).toDouble
        val lat = row.getString(2).toDouble
        val tid = row.getString(3).toInt

        val point = G.createPoint(new Coordinate(lon, lat))
        point.setUserData(Data(oid, tid))
        point
      }
      .cache()
    val n = pointsRDD.count()
    logger.info(s"Total points loaded: $n")

    val (quadtree, cells, rtree, universe) =
      Quadtree.build(pointsRDD, capacity = params.scapacity(), fraction = params.fraction())
    logger.info(s"Quadtree built with ${cells.size} cells")

    saveAsTSV(
      "/tmp/Q.wkt",
      cells.values.map { cell =>
        val cid = cell.id
        val wkt = cell.wkt

        s"$wkt\t$cid\n"
      }.toList
    )

    val pointsSRDD = pointsRDD
      .mapPartitions { iter =>
        iter.map { point =>
          val cid = rtree
            .query(point.getEnvelopeInternal())
            .asScala
            .head
            .asInstanceOf[Int]
          (cid, point)
        }
      }
      .partitionBy(SimplePartitioner(cells.size))
      .map(_._2)
      .cache()
    val count = pointsSRDD.count()
    logger.info(s"Points repartitioned into SRDD with $count points")
    
    val THist = pointsSRDD
      .mapPartitions { it =>
        val partitionId = TaskContext.getPartitionId()
        it.map { point =>
          point.getUserData().asInstanceOf[Data].tid
        }.toList
          .groupBy(tid => tid)
          .map { case (tid, list) =>
            (tid, list.size)
          }
          .toIterator
      }
      .groupByKey()
      .map { case (tid, counts) =>
        val total = counts.sum
        Bin(tid, total)
      }
    logger.info("Temporal histogram (THist) computed")

    saveAsTSV(
      "/tmp/THist.tsv",
      THist
        .collect()
        .sortBy(_.instant)
        .map(bin => s"${bin.toString()}\n")
        .toList
    )

    implicit val intervals = groupInstants(THist.collect().sortBy(_.instant).toSeq, capacity = params.tcapacity())
      .map{ group =>
        val begin = group.head.instant
        val end = group.last.instant
        val number_of_times = group.map(_.count).sum
        
        (begin, end, number_of_times)
      }
      .zipWithIndex
      .map{ case(interval_prime, index) =>
        val begin = interval_prime._1
        val end = interval_prime._2
        val number_of_times = interval_prime._3

        index -> Interval(index, begin, end, number_of_times)
      }.toMap
    //val temporal_bounds = intervals.values.map(interval => (interval.begin, interval.index)).toList.sortBy(_._1)
    val temporal_bounds = intervals.values.map(_.begin).toArray.sorted
    logger.info("Instants grouped based on maximum sum")

    saveAsTSV(
      "/tmp/intervals.tsv",
      intervals.values.toList.sortBy(_.index).map{ interval =>
        interval.toText
      }
    )

    saveAsTSV(
      "/tmp/cells.tsv",
      cells.values.toList.sortBy(_.id).map{ cell =>
        cell.toText
      }
    )

    val pointsSTRDD_prime = pointsSRDD.mapPartitionsWithIndex{ (s_index, it) =>
      it.map { point =>
        val data = point.getUserData().asInstanceOf[Data]
        val tid = data.tid
        val oid = data.oid

        val t_index = Interval.findInterval(temporal_bounds, tid).index
        val st_index = BitwisePairing.encode(s_index, t_index)

        (st_index, point)
      }
    }.cache()

    val st_indexes = pointsSTRDD_prime
      .map{ case (st_index, point) => st_index }
      .distinct()
      .collect()
      .zipWithIndex.toMap
    logger.info(s"Total distinct ST_Indexes: ${st_indexes.size}")
    val st_indexes_reverse = for ((k,v) <- st_indexes) yield (v, k)
    
    val pointsSTRDD = pointsSTRDD_prime
      .map{ case (st_index, point) => (st_indexes(st_index), point) }
      .partitionBy(SimplePartitioner(st_indexes.size))
      .map(_._2)
      .cache()
    val nPointsSTRDD = pointsSTRDD.count()
    logger.info(s"Points repartitioned into STRDD with $nPointsSTRDD points")

    pointsSTRDD.mapPartitions{ points =>
      val st_index = st_indexes_reverse(TaskContext.getPartitionId)
      val (s_index, t_index) = BitwisePairing.decode(st_index)
      points.map{ point =>
        val data = point.getUserData.asInstanceOf[Data]
        val i = data.oid
        val t = data.tid
        val x = point.getX
        val y = point.getY
        val wkt = point.toText

        (s_index, s"$i\t$x\t$y\t$t\t$s_index\t$wkt\n")
      }
    }
      .groupBy{_._1}.collect
      .map{ case(s_index, it) =>
        saveAsTSV(
          s"/tmp/STP/P_${s_index}.wkt",
          it.map(_._2).toList
        )
      }
   
    pointsSTRDD.mapPartitionsWithIndex{ (st_index_prime, it) =>
      val st_index = st_indexes_reverse(st_index_prime)
      val (s_index, t_index) = BitwisePairing.decode(st_index)
      val cell = cells(s_index)
      val interval = intervals(t_index)
      val wkt = cell.wkt
      val beg = interval.begin
      val dur = interval.duration

      Iterator( (s_index, s"$wkt\t$beg\t$dur\n") )
    }.groupBy{_._1}.collect
    .map{ case(s_index, it) =>
      saveAsTSV(
        s"/tmp/STP/C_${s_index}.wkt",
        it.map(_._2).toList
      )
    }

    spark.close
    logger.info("SparkSession closed")
  }

  def gaussianSeries(
      n: Int = 1000,
      min: Double = 0.0,
      max: Double = 1.0
  ): List[Double] = {
    val data = (0 to n).toList.map(_ => Random.nextGaussian())
    rescaleList(data, min, max)
  }

  def rescaleList(
      data: List[Double],
      newMin: Double,
      newMax: Double
  ): List[Double] = {
    if (data.isEmpty) return List.empty
    val currentMin = data.min
    val currentMax = data.max
    if (currentMin == currentMax) {
      return data.map(_ => newMin)
    }
    data.map { x =>
      val normalized = (x - currentMin) / (currentMax - currentMin)
      normalized * (newMax - newMin) + newMin
    }
  }

  def generateGaussianPointset(
      n: Int,
      x_limit: Double = 5000.0,
      y_limit: Double = 5000.0,
      t_limit: Double = 1000.0
  )(implicit G: GeometryFactory, spark: SparkSession): RDD[Point] = {

    val Xs = gaussianSeries(n, 0.0, x_limit)
    val Ys = gaussianSeries(n, 0.0, y_limit)
    val Ts = gaussianSeries(n, 0.0, t_limit).map(_.toInt)
    val points = (0 to n).map { i =>
      val x = Xs(i)
      val y = Ys(i)
      val t = Ts(i)
      val point = G.createPoint(new Coordinate(x, y))
      point.setUserData(Data(i, t))
      point
    }

    saveAsTSV(
      "/tmp/P.wkt",
      points.map { point =>
        val x = point.getX
        val y = point.getY
        val t = point.getUserData().asInstanceOf[Data].tid
        val i = point.getUserData().asInstanceOf[Data].oid
        val wkt = point.toText()

        f"$i%d\t$x%.3f\t$y%.3f\t$t%d\t$wkt\n"
      }.toList
    )

    spark.sparkContext.parallelize(points)
  }

  def saveAsTSV(filename: String, content: Seq[String]): Unit = {
    import java.io._
    val pw = new PrintWriter(new File(filename))
    pw.write(content.mkString(""))
    pw.close()
    logger.info(s"Saved ${content.size} records to $filename") 
  }

  /**
   * Groups a sequence of bins (instants and their counts) into sub-sequences from left to right, 
   * such that the sum of each group does not exceed a maximum limit.  
   * The process is greedy: it maximizes the size of the current group before starting a new one. 
   * The relative order of elements is preserved.
   *
   * @param numbers The input sequence of integers.
   * @param capacity The maximum allowed sum for any group.
   * @return A list of lists, where each inner list is a valid group (except for elements that 
   * individually exceed the maxSum, which are handled with a warning).
   */
  def groupInstants(numbers: Seq[Bin], capacity: Double): List[List[Bin]] = {
    // We use a foldLeft to process the list sequentially and maintain state.
    // The state (accumulator) is a tuple: (completedGroups, currentGroup, currentSum)
    val initialState = (List.empty[List[Bin]], List.empty[Bin], 0)

    val (completedGroups, finalGroup, _) = numbers.foldLeft(initialState) {
      case ((groups, currentGroup, currentSum), bin) =>
        
        // 1. Sanity Check: If an individual number exceeds the maximum sum,
        // it must form its own group. We complete the current group and start 
        // a new one containing only the violating number.
        val number = bin.count
        if (number > capacity) {
           //println(s"Warning: Element $number exceeds maxSum $maxSum. It will be placed in a separate group.")
           val updatedGroups = if (currentGroup.nonEmpty) groups :+ currentGroup else groups
           // Start a new group with the single, violating number and immediately complete it.
           (updatedGroups :+ List(bin), List.empty[Bin], 0)
        }
        
        // 2. Standard case: Check if adding the number is within the limit
        else if (currentSum + number <= capacity) {
          // If the sum is within the limit, add the number to the current group
          // Note: using :+ on List for simple append is fine for small lists, 
          // but for highly performance-critical code on very large lists, 
          // one might prefer prepending and reversing at the end.
          (groups, currentGroup :+ bin, currentSum + number)
        } else {
          // 3. Limit exceeded: The current group is complete.
          // Start a new group with the current number.
          (groups :+ currentGroup, List(bin), number)
        }
    }

    // After folding, we must add the last group being built (finalGroup), if it's not empty.
    if (finalGroup.nonEmpty) completedGroups :+ finalGroup else completedGroups
  }

  // Case Classes...

  case class Data(oid: Int, tid: Int)

  /**
    * A bin of a histogram that count how many entries per instant in a sequence.
    *
    * @param instant A particular instant.
    * @param count The count of entries in this instant.
    */
  case class Bin(instant: Int, count: Int){
    override def toString(): String = s"[$instant, $count]"
  }

  case class SimplePartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions
    override def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }
}

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val filename = "/opt/Research/Datasets/gaussian/P25K.wkt"
  val input: ScallopOption[String] = opt[String](default = Some(filename))
  val master: ScallopOption[String] = opt[String](default = Some("local[3]"))
  val epsilon: ScallopOption[Double] = opt[Double](default = Some(10.0))
  val mu: ScallopOption[Int] = opt[Int](default = Some(3))
  val tolerance: ScallopOption[Double] = opt[Double](default = Some(1e-3))
  val fraction: ScallopOption[Double] = opt[Double](default = Some(0.1))
  val scapacity: ScallopOption[Int] = opt[Int](default = Some(200))
  val tcapacity: ScallopOption[Int] = opt[Int](default = Some(2000))

  verify()
}
