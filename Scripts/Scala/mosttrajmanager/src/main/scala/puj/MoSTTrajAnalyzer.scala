package puj

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.locationtech.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}

import scopt.OParser

import puj.Utils._

object MoSTTrajAnalyzer extends Logging {

  /** Run the Spark job with the given settings.
    *
    * @param S
    */
  def runSparkJob(S: Settings): Unit = {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("MoST Trajectory Analyzer")
      .master(S.master)
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()

    import spark.implicits._

    logger.info(s"Reading ${S.input}")

    val trajs  = spark.read
      .textFile(S.input)
      .repartition(200)
      .cache()
    val nTrajs = trajs.count()
    logger.info(s"Data=${nTrajs}")

    val oids: Map[String, Long] = trajs
      .mapPartitions { rows =>
        rows.map { row =>
          val arr = row.split("\t")
          arr(0)
        }
      }
      .distinct()
      .collect()
      .zipWithIndex
      .map { case (oid, index) =>
        (oid, index.toLong)
      }
      .toMap
    val nOids                   = oids.size
    logger.info(s"OIDs=${nOids}")

    val points: Dataset[STPoint] = trajs
      .mapPartitions { rows =>
        rows.map { row =>
          val arr = row.split("\t")
          val oid = oids(arr(0)).toLong
          val lon = arr(1).toDouble
          val lat = arr(2).toDouble
          val tid = arr(3).toInt / 300

          STPoint(oid, lon, lat, tid)
        }
      }
      .repartition($"oid")
      .cache()
    val nPoints                  = points.count()
    logger.info(s"Points=${nPoints}")

    if (S.debug) {
      points.show(10, truncate = false)
      save("/tmp/histogram.tsv") {
        points
          .groupByKey(_.tid)
          .mapGroups { case (tid, points) =>
            val time  = tid / 3600
            val count = points.size
            s"$time\t$count\n"
          }
          .collect()
          .sortBy(_.split("\t")(0).toInt)
      }

      save("/tmp/trajectories.wkt") {
        points
          .mapPartitions { rows =>
            val wktWriter = new WKTWriter(3)
            rows.toList
              .groupBy(_.oid)
              .map { case (oid, points) =>
                val coords = points
                  .sortBy(_.tid)
                  .map { point =>
                    new Coordinate(point.lon, point.lat, point.tid.toDouble)
                  }
                  .toArray
                try {
                  val lineString = S.geofactory.createLineString(coords)
                  val wkt        = wktWriter.write(lineString)
                  s"$wkt\t$oid\n"
                } catch {
                  case e: IllegalArgumentException =>
                    ""
                }
              }
              .filter(wkt => !wkt.isEmpty)
              .toIterator
          }
          .sample(false, 0.1, 42L)
          .collect()
      }
    }

    save("/opt/Datasets/MoST/MoST.tsv") {
      points.sort($"tid", $"oid").map(_.wkt).collect()
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
