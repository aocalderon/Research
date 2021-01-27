package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import org.locationtech.jts.geom.{Coordinate, Point}
import org.locationtech.jts.io.WKTReader
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.alg.clique.BronKerboschCliqueFinder
import org.apache.commons.math3.geometry.euclidean.twod.DiskGenerator
import scala.collection.JavaConverters._
import java.io.PrintWriter

import edu.ucr.dblab.pflock.Utils._

object FF {

  def computePairs(points: List[Point], epsilon: Double): List[(Point, Point)] = {
      for {
        a <- points
        b <- points if {
          val id1 = a.getUserData.asInstanceOf[Int]
          val id2 = b.getUserData.asInstanceOf[Int]
            (id1 < id2) && (a.distance(b) <= epsilon)
        }
      } yield {
        (a, b)
      }
  }

  def computeCenters(pairs: List[(Point, Point)], r2: Double)
    (implicit geofactory: GeometryFactory, tolerance: Tolerance): List[Point] = {

    pairs.map{ case(p1, p2) =>
      calculateCenterCoordinates(p1, p2, r2)
    }.flatten
  }

  def getDisks(points: List[Point], centers: List[Point], r: Double, mu: Int):
      List[Disk] = {

    val join = for {
      c <- centers
      p <- points if c.distance(p) <= r
    } yield {
      (c, p)
    }

    join.groupBy(_._1).map{ case(center, points) =>
      val pids = points.map(_._2.getUserData.asInstanceOf[Int]).toList
      val support = center.getUserData.asInstanceOf[List[Int]]

      Disk(center, pids, support)
    }.filter(_.pids.size >= mu).toList
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new Params(args)
    implicit val geofactory = new GeometryFactory(new PrecisionModel(params.scale()))
    implicit val tolerance = Tolerance(1.0 / geofactory.getPrecisionModel.getScale)
    implicit val degugOn = params.debug()

    val input = params.input()
    val epsilon_prime = params.epsilon()
    val epsilon = epsilon_prime + tolerance.value
    val r = (epsilon_prime / 2.0) + tolerance.value
    val r2 = math.pow(epsilon_prime / 2.0, 2) + tolerance.value
    val mu = params.mu()

    val points = timer{"Reading points"}{
      readPoints(input)
    }

    val pairs = timer{"Getting pairs"}{
      computePairs(points, epsilon)
    }

    val centers = timer{"Getting centers"}{
      computeCenters(pairs, r2)
    }

    val disks = timer{"Getting disks"}{
      getDisks(points, centers, r, mu)
    }

    val maximals = timer{"Getting maximals"}{
      pruneDisks2(disks)
    }

    debug{
      implicit val data = Data(
        points   = points,
        pairs    = pairs,
        centers  = centers,
        disks    = disks,
        maximals = maximals
      )
      saveData
    }
  }
}
