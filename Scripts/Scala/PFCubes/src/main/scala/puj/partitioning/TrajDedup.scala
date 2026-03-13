package puj.partitioning

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKTReader, WKTWriter}

import puj.Utils._
import puj.{Setup, Settings}
import scala.util.Random

object TrajDedup extends Logging {

  def main(args: Array[String]): Unit = {
    implicit var S: Settings        = Setup.getSettings(args) // Initializing settings...
    implicit val G: GeometryFactory = S.geofactory            // Initializing geometry factory...

    // Starting Spark...
    implicit val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(S.master)
      .appName("PFlock")
      .getOrCreate()

    S.appId = spark.sparkContext.applicationId
    logger.info(s"${S.appId}|START|Starting TrajDedup computation")
    S.printer

    val trajs = spark.read // Reading trajectories...
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .mapPartitions {
        rows =>
          rows.map {
            row =>
              val oid = row.getString(0).toInt
              val lon = addNoise(row.getString(1).toDouble, 3)
              val lat = addNoise(row.getString(2).toDouble, 3)
              val tid = row.getString(3).toInt

              val point = G.createPoint(new Coordinate(round(lon, 3), round(lat, 3)))
              // val point = G.createPoint(new Coordinate())
              point.setUserData(Data(oid, tid))

              point
          }
      }
      .cache
    val nTrajs =trajs.count()
    logger.info(s"${S.appId}|INFO|Read $nTrajs trajectories")

    trajs.take(10).foreach {
      p =>
        val data = p.getUserData.asInstanceOf[Data]
        logger.info(s"${S.appId}|SAMPLE|Trajectory ${data.oid} at time ${data.tid} at (${p.getX}, ${p.getY})")
    }

    val duplicates = trajs.groupBy(p => (p.getCoordinate().x, p.getCoordinate().y))
      .mapValues(_.size)
      .filter(_._2 > 1)
      .cache()
    val nDuplicates = duplicates.count()
    logger.info(s"${S.appId}|RESULT|Found $nDuplicates duplicate trajectories")

    val duplicateDistribution = duplicates
      .map { case (_, count) => (count, 1) }
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()

    logger.info(s"${S.appId}|RESULT|Duplicates Distribution:")
    logger.info(s"${S.appId}|RESULT|#Duplicates\t#Points")
    duplicateDistribution.foreach {
      case (count, numPoints) =>
        logger.info(s"${S.appId}|RESULT|$count\t$numPoints")
    }

    spark.stop()
    logger.info(s"${S.appId}|END|TrajDedup computation finished")
  }
}
