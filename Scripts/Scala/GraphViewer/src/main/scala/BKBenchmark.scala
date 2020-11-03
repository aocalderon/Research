package edu.ucr.dblab

import scala.io.Source
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Geometry, Coordinate, Point}
import com.vividsolutions.jts.io.WKTReader
import scala.collection.JavaConverters._
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import java.io.PrintWriter
import scala.io.Source
import CliqueFinderUtils.clocktime

object BKBenchmark {
  case class Clique(id: Int, points: List[Point])

  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)
    val reader = new WKTReader()
    val precision = 1 / model.getScale
    val input = args(0)
    val distance = args(1).toDouble
    val epsilon = distance + precision
    val method = args(2).toInt

    val buffer = Source.fromFile(input)
    val points = buffer.getLines.zipWithIndex.toList
      .map{ line =>
        val arr = line._1.split("\t")
        val id = arr(0).toInt
        val x = arr(1).toDouble
        val y = arr(2).toDouble
        val point = geofactory.createPoint(new Coordinate(x, y))

        point.setUserData(id)
        point
      }
    buffer.close

    val pairs = for {
      a <- points
      b <- points if {
        val id1 = a.getUserData.asInstanceOf[Int]
        val id2 = b.getUserData.asInstanceOf[Int]
          (id1 < id2) && (a.distance(b) <= epsilon)
      }
    } yield {
      (a, b)
    }

    val graph = new SimpleGraph[Geometry, DefaultEdge](classOf[DefaultEdge])
    points.foreach{ vertex =>  graph.addVertex(vertex) }
    pairs.foreach{ case(a, b) => graph.addEdge(a, b) }

    val (finder, alg) = method match {
      case 1 => (new BKCliqueFinder(graph), "Base")
      case 2 => (new PBKCliqueFinder(graph),"Pivot")
      case 3 => (new DBKCliqueFinder(graph),"Degeneracy")
    }

    val t0 = clocktime
    val cliques = finder.iterator.asScala.toList.map{
      _.asScala.toList.map(_.getUserData.asInstanceOf[Int]).sorted
    }
    val t1 = clocktime
    println(
      s"%-10s|%d|%d|%6.2f".format(alg, distance.toInt, cliques.size, (t1 - t0) / 1e9)
    )

    val f = new PrintWriter(s"/tmp/BK_${alg}_E${distance}.txt")
    f.write(
      cliques.map(clique => s"${clique.mkString(" ")}\n").sorted.mkString("")
    )
    f.close
  }
}
