package puj

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions._

import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.locationtech.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}

import scopt.OParser

import puj.Utils._

object MoSTTrajExtractor extends Logging {
  
  case class Traj(oid: Long, line: String, start: Int, duration: Int){
    override def toString: String = s"${line}\t${oid}\t${start}\t${duration}"

    def wkt(): String = s"${toString}\n"
  }

      /**
   * Run the Spark job with the given settings.
   *
   * @param S
   */
  def runSparkJob(S: Settings): Unit = {
    val minimum_duration = 54 * 3 

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("MoST Trajectory Extractor")
      .master(S.master)
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()

    import spark.implicits._

    logger.info(s"Reading ${S.input}")

    try {
      val data = spark.read
        .textFile(S.input)
        .cache()
      val nData = data.count()
      logger.info(s"Data=${nData}")

      val points = data
        .map { row =>
            val arr = row.split("\t")
            val oid = arr(0).toInt
            val lon = arr(1).toDouble
            val lat = arr(2).toDouble
            val tid = arr(3).toInt
            
            STPoint(oid, lon, lat, tid)
        }
        .repartition($"oid")
        .cache()
      val nPoints = points.count()
      logger.info(s"Points=${nPoints}")

      val A: Dataset[List[STPoint]] = points.mapPartitions{ iter =>
        iter.toList.groupBy(_.oid).flatMap{ case (oid, list) =>
          val sorted = list.sortBy(_.tid)
          val splitted: List[List[STPoint]] = Utils.splitSTPointByGap(sorted).filter(_.length >= minimum_duration).map{ segment =>
            segment.zipWithIndex
            .filter{ case (_, index) =>
              index % 54 == 0
            }
            .map{ case (point, index) =>
              point.copy(tid = index / 54)
            }
          }

          val sample: List[List[STPoint]] = splitted.flatMap{ segment =>
            Utils.splitSTPointByStatic(segment).filter(_.length > 3)
          }

          sample
        }.toIterator
      }.cache()
      val nA = A.count()
      logger.info(s"Pointsets=${nA}")

      val trajs2 = A.rdd.zipWithUniqueId()
        .map { case(points: List[STPoint], oid: Long) => 
            
            val coords = points.map{ point =>
              point.copy(oid = oid)
              new Coordinate(point.lon, point.lat, point.tid)
            }.toArray
            val traj = S.geofactory.createLineString(coords)
            val wkt = new WKTWriter(3).write(traj)
            val beg = coords.head.getZ.toInt
            val end = coords.last.getZ.toInt
            val dur = (end - beg).toInt
            Traj(oid, wkt, beg, dur)
        }
        .cache()
      val nTrajs2 = trajs2.count()
      logger.info(s"Trajectories2=${nTrajs2}")
      save("/opt/Datasets/MoST2.wkt"){
        trajs2.collect().toList.sortBy(_.oid).map{_.wkt()}
      }

      val trajs = points.groupByKey(_.oid).flatMapGroups{ case (oid, iter) =>
        val sorted = iter.toArray.sortBy(_.tid)
        val coords = sorted.map{ point =>
          val lon = point.lon
          val lat = point.lat
          val tid = point.tid
          new Coordinate(lon, lat, tid)
        }.toArray
        
        try{
          Utils.splitByGap(coords.toList).filter(_.length >= minimum_duration).map{ valid =>
            val sorted = valid.sortBy(_.getZ)
            .zipWithIndex
            .filter{ case (coord, index) =>
              index % 54 == 0
            }
            .map{ case (coord, index) =>
              new Coordinate(coord.getX, coord.getY, index / 54)
            }
            .toArray
            val traj = S.geofactory.createLineString(sorted)
            val wkt = new WKTWriter(3).write(traj)
            val beg = sorted.head.getZ.toInt
            val end = sorted.last.getZ.toInt
            val dur = (end - beg).toInt
            Traj(oid, wkt, beg, dur)
          }
        } catch {
          case e1: java.lang.IllegalArgumentException => 
            logger.error(s"Failed to create LineString for OID=${oid}: ${e1.getMessage}")
            List.empty[Traj]
          case e2: java.util.NoSuchElementException => 
            logger.error(s"Failed to create LineString for OID=${oid}: ${e2.getMessage}")
            List.empty[Traj]
        }
      }.cache()
      val nTrajs = trajs.count()
      logger.info(s"Trajectories=${nTrajs}")

      val trajs_prime = trajs.mapPartitions{ T =>
        val reader = new WKTReader(S.geofactory)
        T.map{ traj =>
          val geom = reader.read(traj.line)
          val nPoints = geom.getNumPoints
          (traj.oid, nPoints, traj.duration)
        }
      }.cache()
      
      val check = trajs_prime.filter(r => r._2 != r._3 + 1).count()
      logger.info(s"Trajectories with inconsistent number of points: nPoints != duration + 1 = ${check}")
      val minimum = trajs_prime.filter(r => r._3 == minimum_duration).count()
      logger.info(s"Trajectories with minimum duration: ${minimum}")

      save("/opt/Datasets/MoST.wkt"){
        trajs.collect().toList.sortBy(_.oid).map{_.wkt()}
      }

    } catch {
      case e: Exception => println(s"Failed to read file: ${e.getMessage}")
    } finally {
      spark.stop()
    }
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
