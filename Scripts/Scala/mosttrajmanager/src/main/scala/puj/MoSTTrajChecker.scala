package puj

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions._

import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.locationtech.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}

import scopt.OParser

import puj.Utils._

object MoSTTrajChecker extends Logging {

  /** Run the Spark job with the given settings.
    *
    * @param S
    */
  def runSparkJob(S: Settings): Unit = {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("MoST Trajectory Checker")
      .master(S.master)
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()

    import spark.implicits._

    logger.info(s"Reading ${S.input}")

    val points  = spark.read
      .textFile(S.input)
      .map { row =>
        val arr = row.split("\t")
        val oid = arr(0).toLong
        val lon = arr(1).toDouble
        val lat = arr(2).toDouble
        val tid = arr(3).toInt

        STPoint(oid, lon, lat, tid)
      }
      .repartition($"oid")
      .sort($"tid", $"oid")
      .cache()
    val nPoints = points.count()
    logger.info(s"Points=${nPoints}")

    val oids  = points.select($"oid").distinct()
    val nOids = oids.count()
    logger.info(s"OIDs=${nOids}")

    val tids  = points.select($"tid").distinct()
    val nTids = tids.count()
    logger.info(s"TIDs=${nTids}")

    save("/tmp/histogram.tsv"){
      points.groupBy("tid").count().orderBy("tid").collect().map{ row =>
        val tid   = row.getInt(0)
        val count = row.getLong(1)
        s"${tid}\t${count}\n"
      }
    }

    val histogram = points.groupBy("oid").count().cache()
    val nTrajs = histogram.count()
    logger.info(s"Trajectories=${nTrajs}")

    val avgLength = histogram.agg(avg($"count")).first().getDouble(0)
    logger.info(f"Average length=${avgLength}%.2f points")
    val maxLength = histogram.agg(max($"count")).first().getLong(0)
    logger.info(s"Maximum length=${maxLength} points")
    val minLength = histogram.agg(min($"count")).first().getLong(0)
    logger.info(s"Minimum length=${minLength} points")
    val stdLength  = histogram.agg(stddev($"count")).first().getDouble(0)
    logger.info(f"Standard deviation=${stdLength}%.2f points")

    histogram.filter($"count" < 3).count() match {
      case 0 => logger.info("No trajectories with 3 points or less")
      case n => logger.warn(s"Trajectories with 3 points or less=${n}")
    }

    val greater3 = histogram.filter($"count" >= 3)

    val sample = points.join(greater3, Seq("oid"))
      .select($"oid", $"lon", $"lat", $"tid")
      .as[STPoint]
      .repartition($"oid")
      .sort($"tid", $"oid")
      .cache()
    val nSamplePoints = sample.count()
    logger.info(s"Sample Points=${nSamplePoints}")

    // save("/tmp/MoST_sample.tsv"){
    //   sample.collect().sortBy(_.tid).map(_.wkt)
    // }

    if (S.debug) {
      points.sort($"tid", $"oid").show(200, truncate = false)
    }

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
