package edu.ucr.dblab.pflock.pbk

import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.Graphs
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel, Point, Coordinate}
import scala.collection.mutable.{ListBuffer, HashSet}
import scala.collection.JavaConverters._

import edu.ucr.dblab.pflock.Utils.save

import edu.ucr.dblab.pflock.pbk.PBK_Utils._
import edu.ucr.dblab.pflock.pbk.PBK.bk

object PBK_Tester {
  def main(args: Array[String]): Unit = {
    implicit val geofactory = new GeometryFactory(new PrecisionModel(1000))
    implicit val graph = new SimpleGraph[Point, DefaultEdge](classOf[DefaultEdge])
    implicit val sortMode = SortMode(2)

    val input   = args(0)
    val epsilon = args(1).toDouble
    val (vertices, edges) = readTrajs(input, epsilon)

    vertices.foreach(graph.addVertex)
    edges.foreach{ case(a, b) => graph.addEdge(a, b) }

    val cliques = bk(vertices, edges)
    cliques.print
  }
}
