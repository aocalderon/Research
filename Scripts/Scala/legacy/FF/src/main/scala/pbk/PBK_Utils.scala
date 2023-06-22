package edu.ucr.dblab.pflock.pbk

import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.Graphs
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Point, Coordinate}
import scala.collection.mutable.{ListBuffer, HashSet}
import scala.io.Source
import scala.collection.JavaConverters._

import edu.ucr.dblab.pflock.FF.computePairs
import edu.ucr.dblab.pflock.Utils.readPoints

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

  def readTrajs(filename: String, epsilon: Double)
    (implicit geofactory: GeometryFactory): (List[Point], List[(Point, Point)]) = {

    val points = readPoints(filename)
    val pairs = computePairs(points, epsilon)

    (points, pairs)
  }
}
