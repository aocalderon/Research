package edu.ucr.dblab.parrouter

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Coordinate, LineString}
import org.locationtech.jts.io.WKTReader

import com.graphhopper.{GraphHopper, ResponsePath, GHRequest, GHResponse}
import com.graphhopper.config.{CHProfile,Profile}
import com.github.nscala_time.time.Imports._

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.io.Source
import java.io.{BufferedWriter, FileWriter}

import ParRouter._

object ParResampler {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  private def lerp(start: Coordinate, end: Coordinate, ratio: Double): Coordinate = {
    def lerp(
      start: Double,
      end: Double,
      ratio: Double): Double = start * (1 - ratio) + end * ratio

    new Coordinate(lerp(start.x, end.x, ratio), lerp(start.y, end.y, ratio))
  }

  private def findRatioBetweenPointsToGetDistanceWanted(
    start: Coordinate,
    end: Coordinate,
    distanceTraveled: Double,
    distanceWanted: Double): Double = {

    val distanceFromStartToEnd = start.distance(end)
    val distanceRemaining = distanceWanted - (distanceTraveled - distanceFromStartToEnd)
    distanceRemaining / distanceFromStartToEnd
  }

  def resample(traj: LineString, step: Double)(implicit G: GeometryFactory): LineString = {
    @annotation.tailrec
    def resample_tailrec(
      coords: Array[Coordinate],
      distanceTraveled: Double,
      distanceWanted: Double,
      resampling_coords: Array[Coordinate]): Array[Coordinate] = {

      if(coords.size == 1){
        resampling_coords
      } else {
        val head = coords.head
        val tail = coords.tail
        val distanceTraveled_prime = distanceTraveled + head.distance(tail.head)
        if( distanceTraveled_prime < distanceWanted ){
          resample_tailrec(tail, distanceTraveled_prime, distanceWanted, resampling_coords)
        } else {
          val ratio = findRatioBetweenPointsToGetDistanceWanted(
            head, tail.head, distanceTraveled_prime, distanceWanted
          )
          val coord_prime = lerp(head, tail.head, ratio)
          resample_tailrec(coord_prime +: tail, 0, distanceWanted, resampling_coords :+ coord_prime)
        }
      }
    }

    val coords = traj.getCoordinates
    val resampled_coords = resample_tailrec(coords, 0.0, step, Array.empty[Coordinate])
    G.createLineString(coords.head +: resampled_coords :+ coords.last)
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new ParRouterParams(args)
    implicit val geofactory = new GeometryFactory(new PrecisionModel(1.0/params.tolerance()))

    logger.info("Reading...")
    val buffer = Source.fromFile(params.dataset())
    val dataset = if(params.noheader()){
      buffer.getLines.toList
    } else {
      val lines = buffer.getLines
      println(lines.next.replaceAll(params.delimiter(), "\t"))
      lines.toList
    }
    buffer.close

    logger.info("Resampling...")
    implicit var progress = new pb.ProgressBar(dataset.size)
    val f = new BufferedWriter(new FileWriter(params.output()), 16384) // Buffer size...
    val reader = new WKTReader(geofactory)
    dataset.par.foreach{ line =>
      val arr = line.split(params.delimiter())
      val lin = reader.read(arr(0)).asInstanceOf[LineString]
      val  id = arr(1)
      val  t1 = arr(2).toLong
      val  t2 = arr(3).toLong

      val route = Route(id, lin, t1, t2, t2 - t1).resampleByTime(params.resample_rate())
      f.write(s"${route.wkt}\n")
      progress.add(1)
    }
    f.close

    logger.info("Closing...")
  }
}
