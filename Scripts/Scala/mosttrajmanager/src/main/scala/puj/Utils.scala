package puj

import org.locationtech.jts.geom._
import org.locationtech.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}

import java.io.PrintWriter
import org.apache.logging.log4j.scala.Logging

object Utils extends Logging {
  case class TrajPoint(oid: String, lon: Double, lat: Double, tid: Long)

  case class Data(oid: String, tid: Long)

  case class Trajectory(oid: String, wkt: String, start: Long, duration: Int){
    override def toString: String = s"${wkt}\t${oid}\t${start}\t${duration}"
  }

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

  // Functions...
  /**
    * Splits a list of points into sub-lists based on static stops.
    *
    * A stop is defined as a sequence of identical points longer than the threshold.
    * Segments are split at stops, and short pauses (<= threshold) are merged into moving segments.
    *
    * @param points Input list of points (assumed to be sorted by tid)
    * @param threshold The minimum number of identical consecutive points to consider a stop
    * @return A List of Lists, where each inner list is a moving segment
    */
  def splitByStatic(points: List[Coordinate], threshold: Int = 3): List[List[Coordinate]] = {

    @annotation.tailrec
    def recurse(
        remaining: List[Coordinate],
        buffer: List[Coordinate],       // Accumulates identical points to check count
        currentSeg: List[Coordinate],   // Accumulates valid moving points
        acc: List[List[Coordinate]]     // Accumulates finished segments
    ): List[List[Coordinate]] = {

      remaining match {
        case Nil =>
          // End of input: Process the final buffer
          if (buffer.size > threshold) {
            // Final points were a static stop -> Discard buffer, finish currentSeg
            if (currentSeg.isEmpty) acc.reverse
            else (currentSeg.reverse :: acc).reverse
          } else {
            // Final points were moving -> Combine buffer + currentSeg
            val finalSeg = (buffer ++ currentSeg).reverse
            if (finalSeg.isEmpty) acc.reverse
            else (finalSeg :: acc).reverse
          }

        case p :: tail =>
          buffer match {
            case Nil =>
              // Initialize: Start buffering the first point
              recurse(tail, List(p), currentSeg, acc)

            case b :: _ =>
              // Compare current point 'p' with the buffered point 'b'
              // (Assumes double precision tolerance is not needed; if so, use math.abs)
              if (p.lon == b.lon && p.lat == b.lat) {
                // Point is static relative to buffer: Add to buffer
                recurse(tail, p :: buffer, currentSeg, acc)
              } else {
                // Point moved: Evaluate the buffer we just finished
                if (buffer.size > threshold) {
                  // CASE 1: The buffer was a STOP (static > threshold)
                  // Action: Split here. Save currentSeg, discard buffer, start new seg with p.
                  val newAcc = if (currentSeg.isEmpty) acc else currentSeg.reverse :: acc
                  recurse(tail, List(p), Nil, newAcc)
                } else {
                  // CASE 2: The buffer was just a pause (static <= threshold)
                  // Action: Keep going. Move buffer to currentSeg, start buffering p.
                  // Note: 'buffer' is reversed, 'currentSeg' is reversed. 
                  // We prepend buffer to currentSeg to maintain reverse order correctly.
                  recurse(tail, List(p), buffer ++ currentSeg, acc)
                }
              }
          }
      }
    }

    recurse(points, Nil, Nil, Nil)
  }

  /**
    * Splits a list of coordinates into sub-lists based on time continuity.
    *
    * @param points Input list of coordinates (assumed to be sorted by tid)
    * @param maxGap The maximum allowed difference in 'tid' to consider points consecutive
    * @return A List of Lists, where each inner list is a continuous segment
    */
  def splitByGap(points: List[Coordinate], maxGap: Int = 1): List[List[Coordinate]] = {

    @annotation.tailrec
    def recurse(
        remaining: List[Coordinate],
        currentSegment: List[Coordinate],
        acc: List[List[Coordinate]]
    ): List[List[Coordinate]] = {
      remaining match {
        // Base case: No more points to process
        case Nil =>
          if (currentSegment.isEmpty) acc.reverse
          else (currentSegment.reverse :: acc).reverse

        // Recursive step
        case nextPoint :: tail =>
          currentSegment match {
            case Nil =>
              // Start the first segment
              recurse(tail, List(nextPoint), acc)

            case lastPoint :: _ =>
              // Check the gap between the next point and the most recent point in the current segment
              val nextTid = nextPoint.getZ.toInt
              val lastTid = lastPoint.getZ.toInt
              if (nextTid - lastTid <= maxGap) {
                // Consecutive: Prepend to current segment (O(1) operation)
                recurse(tail, nextPoint :: currentSegment, acc)
              } else {
                // Gap found: Finalize current segment and start a new one
                recurse(tail, List(nextPoint), currentSegment.reverse :: acc)
              }
          }
      }
    }

    recurse(points, Nil, Nil)
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
