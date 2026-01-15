package puj

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.locationtech.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}

import scopt.OParser

import puj.Utils._

object MoSTTrajChecker extends Logging {
  /**
   * Run the Spark job with the given settings.
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

    val points = spark.read
        .textFile(S.input)
        .map{ row =>
          val arr = row.split("\t")
          val oid = arr(0).toLong
          val lon = arr(1).toDouble
          val lat = arr(2).toDouble
          val tid = (arr(3).toInt / 300) - 49

          STPoint(oid, lon, lat, tid)
        }
        .repartition($"oid")
        .sort($"tid", $"oid")
        .cache()
    val nPoints = points.count()
    logger.info(s"Points=${nPoints}")

    val oids = points.select($"oid").distinct()
    val nOids = oids.count()
    logger.info(s"OIDs=${nOids}")

    save(s"${S.output}/MoST.tsv"){
      points.collect().sortBy(_.tid).map(_.wkt)
    }

    if(S.debug){
      points.sort($"tid", $"oid").show(200, truncate = false)
      save("/tmp/histogram.tsv"){
        points.groupByKey(_.tid).mapGroups{ case (tid, points) =>
          val time = tid
          val count = points.size
          s"$time\t$count\n"
        }.collect().sortBy(_.split("\t")(0 ).toInt)
      }
    }

    spark.stop()
  }

   /**
   * Main entry point.
   *
   * @param args
   */
  def main(args: Array[String]): Unit =
    // Parse command-line arguments
    OParser.parse(Setup.parser, args, Settings()) match {
      case Some(settings) =>
        runSparkJob(settings)
      case _ =>
        System.exit(1)
    }
}