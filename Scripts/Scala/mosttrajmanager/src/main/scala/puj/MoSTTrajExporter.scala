package puj

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.locationtech.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}

import scopt.OParser

import puj.Utils._

object MoSTTrajExporter extends Logging {
  /**
   * Run the Spark job with the given settings.
   *
   * @param S
   */
  def runSparkJob(S: Settings): Unit = {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("MoST Trajectory Exporter")
      .master(S.master)
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()

    import spark.implicits._

    logger.info(s"Reading ${S.input}")

    val trajs = spark.read
        .textFile(S.input)
        .repartition(200)
        .cache()
    val nTrajs = trajs.count()
    logger.info(s"Data=${nTrajs}")

    val points: Dataset[STPoint] = trajs.mapPartitions { rows =>
        val reader = new WKTReader()
        rows.flatMap { row =>
          val arr = row.split("\t")
          val line = arr(0)
          val oid = arr(1).toLong

          val geom = reader.read(line).asInstanceOf[LineString]
          val coords = (0 until geom.getNumPoints).map(i => geom.getCoordinateN(i))
          coords.map(coord => STPoint(oid, coord.getX, coord.getY, coord.getZ.toInt))
        }
      }
      .cache()
    val nPoints = points.count()
    logger.info(s"Points=${nPoints}")

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