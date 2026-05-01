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

object TrajTransform extends Logging {
  case class STPoint_prime(oid: String, lon: Double, lat: Double, tid: Long) {
    override def toString: String = s"${oid}\t${lon}\t${lat}\t${tid}"
  }

  case class Data(oid: Long, tid: Long)

  // Projection transformers (lazy to initialize once)
  private lazy val crsFactory  = new CRSFactory()
  private lazy val ctFactory   = new CoordinateTransformFactory()
  private lazy val transformer = ctFactory.createTransform(
    crsFactory.createFromName("EPSG:4326"),
    crsFactory.createFromName("EPSG:3944")
  )

  // ThreadLocal to reuse ProjCoordinate instances and avoid allocation pressure
  private val threadLocalCoords = new ThreadLocal[(ProjCoordinate, ProjCoordinate)] {
    override def initialValue(): (ProjCoordinate, ProjCoordinate) = 
      (new ProjCoordinate(), new ProjCoordinate())
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

  case class STPoint(oid: Long, lon: Double, lat: Double, tid: Long) {
    override def toString: String = s"${oid}\t${lon}\t${lat}\t${tid}"

    def getPoint(implicit G: GeometryFactory): Point = {
      val point = G.createPoint(transform3d(lon, lat, tid))
      point.setUserData(Data(oid, tid))
      point
    }

    def wkt(implicit G: GeometryFactory): String = {
      val wktWriter = new WKTWriter()
      wktWriter.write(getPoint)
    }
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
    logger.info(s"${S.appId}|START|Starting ${this.getClass.getSimpleName()} computation")
    S.printer

    val trajs = spark.read // Reading trajectories...
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .cache()
    val nTrajs = trajs.count()
    logger.info(s"INFO|Read $nTrajs trajectories")

    val pointsRDD_4326 = trajs.mapPartitions{ rows =>
        rows.map{ row =>
          val oid = row.getString(0)
          val lon = row.getString(1).toDouble
          val lat = row.getString(2).toDouble
          val tid = row.getString(3).toLong

          STPoint_prime(oid, lon, lat, tid)
        }
      }
      .cache()

    val nPoints = pointsRDD_4326.count()
    logger.info(s"INFO|Extracted $nPoints points in EPSG:4326")

    val distinctOids: Map[String, Long] = pointsRDD_4326.map(_.oid)
      .distinct()
      .zipWithUniqueId()
      .collect()
      .toMap
    logger.info(s"INFO|Mapped ${distinctOids.size} distinct OIDs to unique IDs")

    val broadcastedDistinctOids = spark.sparkContext.broadcast(distinctOids)

    val pointsRDD_3944 = pointsRDD_4326.map{ point =>
        val oidLong = broadcastedDistinctOids.value(point.oid)
        val transformedPoint = transform3d(point.lon, point.lat, point.tid)

        STPoint(oidLong, transformedPoint.x, transformedPoint.y, point.tid)
      }
      .cache()
    val nTransformedPoints = pointsRDD_3944.count()
    logger.info(s"INFO|Transformed $nTransformedPoints points to EPSG:3944")

    pointsRDD_3944.map{_.toString()}
      .saveAsTextFile(S.output)

    spark.stop()
    logger.info(s"${S.appId}|END|${this.getClass.getSimpleName()} computation finished")
  }
}
