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

import puj.Utils._

object P3D extends Logging {
  
  def main(args: Array[String]): Unit = {
    logger.info("Starting P3D application")
    implicit val params = new Params(args)
    //implicit var settings = Settings()
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
      Quadtree.build(pointsRDD, new Envelope(), capacity = params.scapacity(), fraction = params.fraction())
    logger.info(s"Quadtree built with ${cells.size} cells")

    saveAsTSV(
      "/tmp/Q.wkt",
      cells.values.map { cell =>
        G.toGeometry(cell.envelope).toText() + "\n"
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

    implicit val intervals = Interval.groupInstants(THist.collect().sortBy(_.instant).toSeq, capacity = params.tcapacity())
      .map{ group =>
        val begin = group.head.instant
        val end = group.last.instant
        val number_of_times = group.map(_.count).sum
        
        (begin, end, number_of_times)
      }.zipWithIndex
      .map{ case ((begin, end, number_of_times), index) =>
        (index, Interval(index, begin, end, number_of_times))
      }.toMap
    logger.info(s"Total temporal intervals created: ${intervals.size}")

    val temporal_bounds = intervals.values.map(_.begin).toArray.sorted

    val pointsSTRDD_prime = pointsSRDD.mapPartitionsWithIndex{ (spatial_index, it) =>
      it.map { point =>
        val data = point.getUserData().asInstanceOf[Data]
        val tid = data.tid

        val temporal_index = Interval.findInterval(temporal_bounds, tid).index
        val ST_Index = BitwisePairing.encode(spatial_index, temporal_index)
        (ST_Index, point)
      }
    }.cache()

    val st_indexes = pointsSTRDD_prime
      .map{ case (st_index, point) => st_index }
      .distinct()
      .collect()
      .zipWithIndex.toMap
    logger.info(s"Total distinct ST_Indexes: ${st_indexes.size}")
    
    val pointsSTRDD = pointsSTRDD_prime
      .map{ case (st_index, point) => (st_indexes(st_index), point) }
      .partitionBy(SimplePartitioner(st_indexes.size))
      .map(_._2)
      .cache()
    val nPointsSTRDD = pointsSTRDD.count()
    logger.info(s"Points repartitioned into STRDD with $nPointsSTRDD points")

    logger.info("SparkSession closed")
  }
}

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val filename = "/opt/Research/Datasets/gaussian/P25K.wkt"
  val input: ScallopOption[String] = opt[String](default = Some(filename))
  val master: ScallopOption[String] = opt[String](default = Some("local[3]"))
  val epsilon_prime: ScallopOption[Double] = opt[Double](default = Some(10.0))
  val mu: ScallopOption[Int] = opt[Int](default = Some(3))
  val scapacity: ScallopOption[Int] = opt[Int](default = Some(200))
  val tcapacity: ScallopOption[Int] = opt[Int](default = Some(200))
  val fraction: ScallopOption[Double] = opt[Double](default = Some(0.1))
  val tolerance: ScallopOption[Double] = opt[Double](default = Some(1e-3))
  val debug: ScallopOption[Boolean] = opt[Boolean](short = 'd', default = Some(false), descr = "Enable debug mode")

  verify()

  def epsilon(): Double = epsilon_prime() + tolerance()
  val r: Double = (epsilon_prime() / 2.0) + tolerance()
  val r2: Double = math.pow(epsilon_prime() / 2.0, 2) + tolerance()
  val expansion: Double = epsilon_prime() * 1.5 + tolerance()

}
