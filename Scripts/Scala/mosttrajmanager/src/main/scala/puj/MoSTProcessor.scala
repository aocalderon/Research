package puj

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTWriter
import org.locationtech.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}

import scopt.OParser

import puj.Utils._

object MoSTProcessor extends Logging {
  case class STPoint(oid: Int, lon: Double, lat: Double, tid: Int)

  /** Run the Spark job with the given settings.
    *
    * @param S
    */
  def runSparkJob(S: Settings): Unit = {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("MoST Trajectory Processor")
      .master(S.master)
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()

    import spark.implicits._

    logger.info(s"Reading ${S.input}")

    try {
      val data  = spark.read
        .textFile(S.input)
        .cache()
      val nData = data.count()
      logger.info(s"Data=${nData}")

      val points  = data
        .map { row =>
          val arr = row.split("\t")
          val oid = arr(0).toInt
          val lon = arr(1).toDouble
          val lat = arr(2).toDouble
          val tid = arr(3).toInt

          STPoint(oid, lon, lat, tid)
        }
        .cache()
      val nPoints = points.count()
      logger.info(s"Points=${nPoints}")

      val tids   = points.map(_.tid).cache()
      val maxTid = tids.reduce((a, b) => Math.max(a, b))
      val minTid = tids.reduce((a, b) => Math.min(a, b))
      val nTids  = tids.distinct().count()
      logger.info(s"TIDs: min=${minTid}, max=${maxTid}, nTIDs=${nTids}")

      val sample = points
        .filter { point =>
          point.tid % 54 == 0
        }
        .cache()

      val lookup = sample.map(_.tid).distinct().collect().zipWithIndex.map { case (tid, idx) => (tid, idx) }.toMap
      logger.info(s"Sampled TIDs: ${lookup.size}")

      save("/opt/Datasets/MoST_sample.tsv") {
        sample.rdd
          .mapPartitions { iter =>
            iter.map { point =>
              val oid    = point.oid
              val lon    = point.lon
              val lat    = point.lat
              val newTid = lookup(point.tid)

              (s"${oid}\t${lon}\t${lat}\t${newTid}\n", newTid)
            }
          }
          .collect()
          .sortBy(_._2)
          .map(_._1)
      }

    } catch {
      case e: Exception => println(s"Failed to read file: ${e.getMessage}")
    } finally
      spark.stop()
  }

  /** Main entry point.
    *
    * @param args
    */
  def main(args: Array[String]): Unit =
    // Parse command-line arguments
    OParser.parse(Setup.parser, args, Settings()) match {
      case Some(settings) =>
        runSparkJob(settings)
      case _              =>
        System.exit(1)
    }
}
