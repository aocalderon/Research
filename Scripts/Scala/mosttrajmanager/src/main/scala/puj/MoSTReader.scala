package puj

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTWriter
import org.locationtech.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}

import scopt.OParser

import puj.Utils._

object MoSTReader extends Logging {

  /**
   * Run the Spark job with the given settings.
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
      val data = spark.read
        .textFile(S.input)
        .filter(line => line.split("\t").size == 5)
      val nData = data.count()
      logger.info(s"Data=${nData}")

      val points = data
        .map { row =>
          val arr = row.split("\t")

          try{
            val tid = arr(0).toDouble.toLong
            val oid = arr(1)
            val lon = arr(2).toDouble
            val lat = arr(3).toDouble

            TrajPoint(oid, lon, lat, tid)

          }catch{
            case e: java.lang.NumberFormatException => {
              TrajPoint("-1", 0.0, 0.0, -1)
            }
          }
        }.filter(_.oid != "-1")
        .cache()
      val nPoints = points.count()
      logger.info(s"Points=${nPoints}")

      val mapper = points.map(_.oid).distinct().rdd.zipWithUniqueId().toDF("oid", "uniqueId")

      val newPoints = points.join(mapper, "oid").sort($"tid")
        .map{ row =>
          val oid = row.getAs[String]("oid")
          val lon = row.getAs[Double]("lon")
          val lat = row.getAs[Double]("lat")
          val newTid = (row.getAs[Long]("tid") / 3)
          val newOid = row.getAs[Long]("uniqueId")

          val coord = transform(lon, lat)
          val x = coord.x
          val y = coord.y

          f"$newOid\t$x%1.3f\t$y%1.3f\t$newTid\n"
        }.collect()
      save(s"${S.output}MoST.tsv"){
        newPoints
      }
      
      if(S.debug) {
        // Show some sample points for debugging...
        val histogram = points.rdd.map(p => (p.tid, 1L))
          .reduceByKey(_ + _)
          .collect()
          .sortBy(_._1)
        save(s"${S.output}histogram.tsv") {
          histogram.map { case (tid, count) =>
            s"$tid\t$count\n"
          }
        }

        // Group by oid and create trajectories...
        val trajs = points
          .groupByKey(_.oid)
          .mapGroups { case (oid, iter) =>
            val writer = new WKTWriter(3)
            val sortedPoints = iter.toArray.sortBy(_.tid)
            val coords = sortedPoints.map { record =>
              transform3d(record.lon, record.lat, record.tid)
            }.toArray
            val line = if (coords.length > 1) {
              S.geofactory.createLineString(coords)
            } else {
              S.geofactory.createEmpty(2)
            }
            
            val start    = sortedPoints.head.tid
            val end      = sortedPoints.last.tid
            val duration = (end - start).toInt
            Trajectory(oid, writer.write(line), start, duration)
          }
          .filter(_.wkt != "LINESTRING EMPTY")
          .cache()
        val nTrajs = trajs.count()
        logger.info(s"Trajs=${nTrajs}")

        // Save trajectories...
        val wkts = trajs
          .map { traj =>
            val wkt      = traj.wkt
            val start    = traj.start
            val duration = traj.duration

            s"$wkt\t$start\t$duration"
          }
          .collect()
          .zipWithIndex
          .map { case (line, idx) =>
            s"${line}\t${idx + 1}\n"
          }

        save(s"${S.output}T.wkt") {
          wkts
        }
      }



    } catch {
      case e: Exception => println(s"Failed to read file: ${e.getMessage}")
    } finally
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
