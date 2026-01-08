package puj

import org.locationtech.jts.geom._
import org.locationtech.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}

import java.io.PrintWriter
import org.apache.logging.log4j.scala.Logging

object Utils extends Logging {
  case class TrajPoint(oid: String, lon: Double, lat: Double, tid: Long)

  case class Data(oid: String, tid: Long)

  case class Trajectory(oid: String, wkt: String, start: Long, duration: Int)

  // Projection transformers (lazy to initialize once)
  private lazy val crsFactory = new CRSFactory()
  private lazy val ctFactory  = new CoordinateTransformFactory()
  private lazy val transformer = ctFactory.createTransform(
    crsFactory.createFromName("EPSG:4326"),
    crsFactory.createFromName("EPSG:3944")
  )

  // ThreadLocal to reuse ProjCoordinate instances and avoid allocation pressure
  private val threadLocalCoords = new ThreadLocal[(ProjCoordinate, ProjCoordinate)] {
    override def initialValue(): (ProjCoordinate, ProjCoordinate) = (new ProjCoordinate(), new ProjCoordinate())
  }

  /**
   * Transform a coordinate from EPSG:4326 -> EPSG:3944.
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

    /**
   * Transform a coordinate from EPSG:4326 -> EPSG:3944.
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

  /**
   * Get current clock time in nanoseconds.
   */
  def clocktime: Long = System.nanoTime()

  /**
   * Save content to a file and log the time taken.
   */
  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f     = new PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end  = clocktime
    val time = "%.2f".format((end - start) / 1e9)
    logger.info(s"Saved ${filename} in ${time}s [${content.size} records].")
  }

}
