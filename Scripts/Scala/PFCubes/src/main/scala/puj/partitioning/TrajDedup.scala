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
  case class STPoint_prime(oid: String, lon: Double, lat: Double, tid: Long) {
    override def toString: String = s"${oid}\t${lon}\t${lat}\t${tid}"
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
    logger.info(s"${S.appId}|START|Starting TrajDedup computation")
    S.printer

    val trajs = spark.read // Reading trajectories...
      .option("header", value = true)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .cache()
    val nTrajs = trajs.count()
    logger.info(s"INFO|Read $nTrajs trajectories")

    val trajsDedup = trajs.filter{ !_.isNullAt(1) }
      .cache()
    val nTrajsDedup = trajsDedup.count()
    logger.info(s"INFO|Deduped $nTrajsDedup trajectories")

    val pointsRDD = trajsDedup.mapPartitions{ rows =>
        rows.flatMap{ row =>
          try{
            val tid = row.getString(0).toDouble.toLong
            val oid = row.getString(1)
            val lon = row.getString(2).toDouble
            val lat = row.getString(3).toDouble

            Some(STPoint_prime(oid, lon, lat, tid))
          } catch {
            case _: Throwable => None
          }
        }
      }
      .cache()
      
    val nPoints = pointsRDD.count()
    logger.info(s"INFO|Extracted $nPoints points")

    pointsRDD.map{_.toString() + "\n"}
      .saveAsTextFile(S.output)

    spark.stop()
    logger.info(s"${S.appId}|END|TrajDedup computation finished")
  }
}
