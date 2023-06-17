package edu.ucr.dblab.pflock.pbk

import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.Graphs
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel, Point, Coordinate}
import scala.collection.mutable.{ListBuffer, HashSet}
import scala.io.Source
import scala.collection.JavaConverters._

import edu.ucr.dblab.pflock.Utils.computePairs

object PBK_Utils {
  case class SortMode(mode: Int)

  def sortByDegree(R: HashSet[Point])
    (implicit graph: SimpleGraph[Point, DefaultEdge]): List[Point] = {

    case class Count(vertex: Point, count: Int)
    R.toList.map{ vertex =>
      Count(vertex, graph.edgesOf(vertex).size())
    }.sortBy(- _.count).map(_.vertex)
  }

  def sortByAngle(R: HashSet[Point])
    (implicit graph: SimpleGraph[Int, DefaultEdge]): List[Point] = {

    ???
  }

  def sortById(R: HashSet[Point]): List[Point] = {
    R.toList.sortBy(_.getUserData.asInstanceOf[Int])
  }

  def getEdges(points: List[Point], epsilon: Double)
    (implicit geofactory: GeometryFactory): List[(Point, Point)] = {

    computePairs(points, epsilon)
  }
}
