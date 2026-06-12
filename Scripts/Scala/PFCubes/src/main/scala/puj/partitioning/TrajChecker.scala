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

object TrajChecker extends Logging {
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

    import spark.implicits._

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

          STPoint(oid, lon, lat, tid)
        }
      }
      .cache()
    val nPointsRDD = pointsRDD.count()
    logger.info(s"INFO|Extracted $nPointsRDD points")

    val checksRDD = pointsRDD.groupBy(_.oid).flatMap{ case (oid, points) =>
      val sortedPoints = points.toArray.sortBy(_.tid)
      val (history, remained) = sortedPoints.splitAt(5)
      val checks = rec_chech(history, remained)

      if (checks.exists(_ == true)) {
        Some(s"Trajectory $oid has a point that is far enough from the previous points.")
      } else {
        None
      }
    }
    .cache()
    val nChecked = checksRDD.count()
    logger.info(s"INFO|Checked $nChecked trajectories")

    spark.stop()
    logger.info(s"${S.appId}|END|${this.getClass.getSimpleName()} computation finished")
  }

  @scala.annotation.tailrec
  def rec_chech(history: Array[STPoint], remained: Array[STPoint], checks: Array[Boolean] = Array.empty)
    (implicit G: GeometryFactory): Array[Boolean] = {
    remained match {
      case Array() => return checks
      case _ =>
        val distances = history.sliding(2).map{ case Array(p1, p2) =>
          p1.getPoint.distance(p2.getPoint)
        }.toList
        val threshold = if (distances.nonEmpty) distances.sum / distances.length else Int.MaxValue
        val point = remained.head
        val check = history.last.getPoint.distance(point.getPoint) > threshold
        //println(s"Checking point ${point.oid} at time ${point.tid}: distance to last point is ${history.last.getPoint.distance(point.getPoint)}, threshold is $threshold, check result is $check  ")
        rec_chech(history.tail :+ point, remained.tail, checks :+ check)
    }
  }
}
