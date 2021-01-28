package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.geom.{Coordinate, Point}
import com.vividsolutions.jts.io.WKTReader
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
    (implicit geofactory: GeometryFactory, settings: Settings): List[Point] = {

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
    implicit val settings = Settings(params.epsilon(), params.mu(),
      tolerance = params.tolerance(),
      debug = params.debug()
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    val input = params.input()
    println(s"Scale:    \t${settings.scale}")
    println(s"Tolerance:\t${settings.tolerance}")
    println(s"Epsilon:  \t${settings.epsilon}")
    println(s"r:        \t${settings.r}")
    println(s"Mu:       \t${settings.mu}")

    val points = timer{"Reading points"}{
      readPoints(input)
    }

    val pairs = timer{"Getting pairs"}{
      computePairs(points, settings.epsilon)
    }

    val centers = timer{"Getting centers"}{
      computeCenters(pairs, settings.r2)
    }

    val disks = timer{"Getting disks"}{
      getDisks(points, centers, settings.r, settings.mu)
    }

    val maximals = timer{"Getting maximals"}{
      pruneDisks2(disks)
    }

    if(settings.debug){
      implicit val data = DataFiles(
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
