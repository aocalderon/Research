package puj.partitioning

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.locationtech.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}

import puj.Utils._
import puj.{Setup, Settings}
import scala.util.Random

object TrajCleaner extends Logging {
  case class OT(oid: String, tid: Int)
  case class Data(oid: Long, tid: Int)
  case class STPoint(oid: Long, lon: Double, lat: Double, tid: Int) {
    val wkt: String = s"${toString()}\n"

    override def toString: String = s"${oid}\t${lon}\t${lat}\t${tid}"
  }

  // Projection transformers (lazy to initialize once)
  private lazy val crsFactory  = new CRSFactory()
  private lazy val ctFactory   = new CoordinateTransformFactory()
  private lazy val transformer = ctFactory.createTransform(
    crsFactory.createFromName("EPSG:4326"),
    crsFactory.createFromName("EPSG:3944")
  )

  // ThreadLocal to reuse ProjCoordinate instances and avoid allocation pressure
  private val threadLocalCoords = new ThreadLocal[(ProjCoordinate, ProjCoordinate)] {
    override def initialValue(): (ProjCoordinate, ProjCoordinate) = (new ProjCoordinate(), new ProjCoordinate())
  }

  def main(args: Array[String]): Unit = {
    implicit var S: Settings        = Setup.getSettings(args) // Initializing settings...
    implicit val G: GeometryFactory = S.geofactory            // Initializing geometry factory...

    // Starting Spark...
    implicit val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.driver.memory", "32g")
      .config("spark.executor.memory", "32g")
      .master(S.master)
      .appName("PFlock")
      .getOrCreate()

    S.appId = spark.sparkContext.applicationId
    logger.info(s"${S.appId}|START|Starting TrajDedup computation")
    S.printer

    import spark.implicits._

    val pointsRDD = spark.read // Reading trajectories...
      .textFile(S.dataset)
      .map{_.split("\t")}
      .filter(_.length == 5)
      .rdd
      .mapPartitions {
        rows =>
          rows.map{ arr =>
            try{
              val tid = arr(0).toDouble.toInt
              val oid = arr(1)
              val lon = arr(2).toDouble
              val lat = arr(3).toDouble

              val point = G.createPoint(transform(lon, lat))
              point.setUserData(OT(oid, tid))

              point
            } catch {
              case e: Exception => {
                val point = G.createPoint(new Coordinate(0.0, 0.0))
                point.setUserData(OT("", -1))

                point
              }
            }
          }.filter{ point =>
              val data = point.getUserData.asInstanceOf[OT]
              data.tid != -1
          }
      }
      .cache
    val nPointsRDD =pointsRDD.count()
    logger.info(s"${S.appId}|INFO|Read $nPointsRDD points")

    val uniqueIDs = pointsRDD.map{ point =>
      val data = point.getUserData.asInstanceOf[OT]
      data.oid
    }
    .distinct()
    .zipWithUniqueId()
    .collect()
    .toMap
    
    val cleanedPoints = pointsRDD.mapPartitions{ points =>
      points.map{ point =>
        val data = point.getUserData.asInstanceOf[OT]
        val newOid = uniqueIDs(data.oid)
        val newPoint = G.createPoint(point.getCoordinate)
        val newTid = (data.tid / 60.0).toInt
        newPoint.setUserData(Data(newOid, newTid))

        newPoint
      }
    }
    .cache
    val nCleanedPoints = cleanedPoints.count()
    logger.info(s"${S.appId}|INFO|Cleaned trajectories have $nCleanedPoints points")

    val points1m = cleanedPoints
      .groupBy{ point => 
        point.getUserData.asInstanceOf[Data]
      }
      .map{ case(data, points) =>
        val n = points.size
        val sumX = points.map(_.getX).sum
        val sumY = points.map(_.getY).sum
        val centroid = G.createPoint(new Coordinate(sumX / n, sumY / n))
        centroid.setUserData(data)

        centroid
      }
      .cache

    points1m
      .map{ point =>
        val data = point.getUserData.asInstanceOf[Data]
        s"${data.oid}\t${point.getX}\t${point.getY}\t${data.tid}"
      }
      .toDS()
      .write
      .mode("overwrite")
      .text(s"${S.output}/dedup")
    logger.info(s"${S.appId}|INFO|Saved deduplicated trajectories to ${S.output}/dedup")

    save("/tmp/most_trajectories.tsv") {
      points1m
        .map{ point =>
          val data = point.getUserData.asInstanceOf[Data]
          STPoint(data.oid, point.getX, point.getY, data.tid)
        }
        .toDS()
        .groupByKey(_.oid)
        .mapGroups { case (oid, pts) =>
          val coords    = pts.toList
            .sortBy(_.tid)
            .map { point =>
              new Coordinate(point.lon, point.lat, point.tid)
            }
            .toArray
          val line = try {
             G.createLineString(coords)
          } catch {
            case e: Exception => {
              logger.error(s"Error creating LineString for oid $oid with ${coords.length} points: ${e.getMessage}")
              G.createLineString(Array.empty[Coordinate])
            }
          }
          val wktWriter = new WKTWriter(3)
          val wkt       = wktWriter.write(line)
          (wkt, oid)
        }
        .collect()
        .sortBy(_._2)
        .map { case (wkt, oid) =>
          s"${wkt}\t${oid}\n"
        }
    }

    spark.stop()
    logger.info(s"${S.appId}|END|TrajDedup computation finished")
  }

  /** Transform a coordinate from EPSG:4326 -> EPSG:3944.
    * Returns (x, y) in target CRS.
    * Optimized to reuse ProjCoordinate containers.
    */
  def transform(lon: Double, lat: Double): Coordinate = {
    val (src, dst) = threadLocalCoords.get()
    src.x = lon
    src.y = lat
    transformer.transform(src, dst)
    new Coordinate(dst.x, dst.y)
  }

  /** Transform a coordinate from EPSG:4326 -> EPSG:3944.
    * Returns (x, y) in target CRS.
    * Optimized to reuse ProjCoordinate containers.
    */
  def transform3d(lon: Double, lat: Double, tid: Long = -1): Coordinate = {
    val (src, dst) = threadLocalCoords.get()
    src.x = lon
    src.y = lat
    transformer.transform(src, dst)
    new Coordinate(dst.x, dst.y, tid.toDouble)
  }  
}
