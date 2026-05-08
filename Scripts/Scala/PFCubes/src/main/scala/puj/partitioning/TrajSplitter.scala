package puj.partitioning

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.locationtech.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}

import puj.Utils._
import puj.{Setup, Settings}

import scala.util.Random

object TrajSplitter extends Logging {
  case class Data(oid: Long, tid: Int)

  case class STPoint(oid: Long, lon: Double, lat: Double, tid: Int) {
    override def toString: String = s"${oid}\t${lon}\t${lat}\t${tid}"

    def getPoint(implicit G: GeometryFactory): Point = {
      val point = G.createPoint(new Coordinate(lon, lat, tid))
      point.setUserData(Data(oid, tid))
      point
    }

    def wkt(implicit G: GeometryFactory): String = {
      val wktWriter = new WKTWriter()
      wktWriter.write(getPoint)
    }
  }

  case class Stats(mean: Double, std: Double, trajectoryId: Long = -1) {
    override def toString: String = s"$trajectoryId\t$mean\t$std"
  }

  def main(args: Array[String]): Unit = {
    implicit var S: Settings        = Setup.getSettings(args) // Initializing settings...
    implicit val G: GeometryFactory = S.geofactory            // Initializing geometry factory...

    // Starting Spark...
    implicit val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.driver.memory",   "32g")
      .config("spark.executor.memory", "32g")
      .master(S.master)
      .appName("PFlock")
      .getOrCreate()

    S.appId = spark.sparkContext.applicationId
    logger.info(s"${S.appId}|START|Starting ${this.getClass.getSimpleName()} computation")
    S.printer

    val pointsRaw = spark.read // Reading trajectories...
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .cache()
    val nPointsRaw = pointsRaw.count()
    logger.info(s"INFO|Read $nPointsRaw points")

    val pointsRDD = pointsRaw.mapPartitions{ rows =>
        rows.map{ row =>
          val oid = row.getString(0).toLong
          val lon = row.getString(1).toDouble
          val lat = row.getString(2).toDouble
          val tid = row.getString(3).toInt

          STPoint(oid, lon, lat, tid).getPoint

        }
      }
      .cache()
    val nPointsRDD = pointsRDD.count()
    logger.info(s"INFO|Extracted $nPointsRDD points")

    val trajs: RDD[Array[Point]] = pointsRDD
      .groupBy(_.getUserData().asInstanceOf[Data].oid)
      .flatMap{ case (oid, points_prime) =>
        val points = points_prime.toArray
          .sortBy(_.getUserData().asInstanceOf[Data].tid)
        val nPoints = points.length
        if (nPoints >= 3) {
          Some(points)
        } else {
          None
        }
      }
      .cache()
    val nTrajs = trajs.count()
    logger.info(s"INFO|Extracted $nTrajs trajectories")

    val validSubTrajs: RDD[Array[Point]] = trajs.flatMap { points =>
        val pairs = points.sliding(2).toArray
        val distances = pairs.map{ pair =>
          val p1 = pair(0)
          val p2 = pair(1)
          p1.distance(p2)
        }
        val meanDistance = distances.sum / distances.length
        val stdDistance = math.sqrt(distances.map(d => math.pow(d - meanDistance, 2)).sum / distances.length)
        val threshold = meanDistance + 1 * stdDistance

        val result = scala.collection.mutable.ArrayBuffer[Array[Point]]()
        var currentTraj = scala.collection.mutable.ArrayBuffer[Point](points.head)

        for (i <- 1 until points.length) {
          if (distances(i - 1) > threshold) {
            if (currentTraj.length >= 3) {
              result += currentTraj.toArray
            }
            currentTraj = scala.collection.mutable.ArrayBuffer[Point](points(i))
          } else {
            currentTraj += points(i)
          }
        }

        if (currentTraj.length >= 3) {
          result += currentTraj.toArray
        }

        result
      }
      .cache()
    val nValidSubTrajs = validSubTrajs.count()
    logger.info(s"INFO|Extracted $nValidSubTrajs valid sub-trajectories")

    import spark.implicits._

    validSubTrajs.zipWithUniqueId.map{ case(traj: Array[Point], oid: Long) =>
        val start = traj.head.getUserData().asInstanceOf[Data].tid
        val end = traj.last.getUserData().asInstanceOf[Data].tid
        val coords = traj.map(_.getCoordinate()).toArray
        val line = G.createLineString(coords)
        line.setUserData(oid)
        
        val wktWriter = new WKTWriter(3)
        val wkt       = wktWriter.write(line)

        s"$wkt\t$oid\t$start\t$end"
      }
      .toDS()
      .write
      .mode("overwrite")
      .text(S.output)

    //pointsRDD_3944.map{_.toString()}.saveAsTextFile(S.output)

    spark.stop()
    logger.info(s"${S.appId}|END|${this.getClass.getSimpleName()} computation finished")
  }
}
