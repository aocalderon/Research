package edu.ucr.dblab

import com.vividsolutions.jts.algorithm.MinimumBoundingCircle
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry, Point}
import com.vividsolutions.jts.io.WKTReader
import scala.collection.JavaConverters._
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.alg.clique.BronKerboschCliqueFinder
import java.io.PrintWriter
import scala.io.Source

object CliqueFinder {
  def main(args: Array[String]): Unit = {

        val model = new PrecisionModel(1000)
        val geofactory = new GeometryFactory(model)
        val reader = new WKTReader()
    val input = "/home/and/Research/tmp/sample.wkt"
    val epsilon = 10

    val buffer = Source.fromFile(input)
    val points = buffer.getLines.zipWithIndex.toList
      .map{ line =>
       
        val point = reader.read(line._1).asInstanceOf[Point]
        val id = line._2
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

    points.foreach{ println }

    pairs.foreach{ case(a, b) => graph.addEdge(a, b) }

    pairs.foreach{println}

    val cliques= {
      val finder = new BronKerboschCliqueFinder(graph)
      finder.iterator.asScala.toList
    }

    val f = new PrintWriter("/tmp/edgesCliques.wkt")
    val clique = cliques.map{ clique =>
      val vertices = geofactory.createMultiPoint(
        clique.asScala.toArray.map(_.getCoordinate)
      )
      val convex = vertices.convexHull
      val mbc = new MinimumBoundingCircle(convex)
      val centroid = geofactory.createPoint(mbc.getCentre)

      s"${convex.toText()}\t${centroid.toText()}\t${mbc.getRadius}\t${convex.getNumPoints}\n"
    }.mkString("")
    f.write(clique)
    f.close
  }
}
