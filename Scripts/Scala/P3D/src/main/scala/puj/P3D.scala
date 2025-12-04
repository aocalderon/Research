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

import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.GeometryItemDistance

import scala.collection.JavaConverters._
import scala.util.Random

object P3D {
  private val logger = LogManager.getLogger("MyLogger")

  case class Data(oid: Int, tid: Int)

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

    // val pointsRDD = spark.read
    //     .option("header", value = false)
    //     .option("delimiter", "\t")
    //     .csv(params.filename)
    //     .rdd
    //     .map{ row =>
    //         val oid = row.getString(0).toInt
    //         val lon = row.getString(1).toDouble
    //         val lat = row.getString(2).toDouble
    //         val tid = row.getString(3).toInt

    //         val point = G.createPoint(new Coordinate(lon, lat))
    //         point.setUserData(Data(oid, tid))
    //         point
    //     }.cache()
    // val n = pointsRDD.count()
    val n: Int = 1000
    val x_limit: Double = 1000.0
    val y_limit: Double = 1000.0
    val t_limit: Double = 100.0
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
        val wkt = point.toText()

        f"$wkt\t$x%.3f\t$y%.3f\t$t%d\n"
      }.toList
    )
    val pointsRDD: RDD[Point] = spark.sparkContext.parallelize(points)

    val (quadtree, cells, universe) =
      Quadtree.build(pointsRDD, new Envelope(), capacity = 50, fraction = 0.5)

    saveAsTSV(
      "/tmp/Q.wkt",
      quadtree.values.map { cell =>
        G.toGeometry(cell).toText() + "\n"
      }.toList
    )

    val pointsSRDD = pointsRDD
      .mapPartitions { iter =>
        iter.map { point =>
          val cid = cells
            .query(point.getEnvelopeInternal())
            .asScala
            .head
            .asInstanceOf[Int]
          (cid, point)
        }
      }
      .partitionBy(SimplePartitioner(quadtree.size))
      .map(_._2)
      .cache()

    val count = pointsSRDD.count()
    logger.info(s"Total points after partitioning: $count")

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
        (tid, total)
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

  def saveAsTSV(filename: String, content: Seq[String]): Unit = {
    import java.io._
    val pw = new PrintWriter(new File(filename))
    pw.write(content.mkString(""))
    pw.close()
  }

  case class SimplePartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions
    override def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }
}

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val filename = "/opt/Research/Datasets/dense2.tsv"
  val input: ScallopOption[String] = opt[String](default = Some(filename))
  val master: ScallopOption[String] = opt[String](default = Some("local[3]"))
  val epsilon: ScallopOption[Double] = opt[Double](default = Some(10.0))
  val mu: ScallopOption[Int] = opt[Int](default = Some(3))
  val tolerance: ScallopOption[Double] = opt[Double](default = Some(1e-3))

  verify()
}
