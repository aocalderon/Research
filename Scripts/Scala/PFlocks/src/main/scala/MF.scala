package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import org.locationtech.jts.geom.{Coordinate, Point}
import org.locationtech.jts.io.WKTReader
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.Graphs
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object MF {
  case class Tolerance(value: Double)

  def main(args: Array[String]): Unit = {
    implicit val params = new Params(args)
    implicit val tolerance = Tolerance(params.tolerance())
    implicit val geofactory = new GeometryFactory(new PrecisionModel(tolerance.value))
    implicit val degugOn = params.debug()

    val input = params.input()
    val epsilon_prime = params.epsilon()
    val epsilon = epsilon_prime + tolerance.value
    val r = (epsilon_prime / 2.0) + tolerance.value
    val r2 = math.pow(epsilon_prime / 2.0, 2) + tolerance.value
    val mu = params.mu()

  }
}
