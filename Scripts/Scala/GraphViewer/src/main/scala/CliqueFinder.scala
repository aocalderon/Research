package edu.ucr.dblab

import com.vividsolutions.jts.algorithm.MinimumBoundingCircle
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.geom.{Coordinate, Point}
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
    val input = "/home/and/Research/Datasets/dense.tsv"
    //val input = "/home/and/Research/tmp/sample.wkt"
    val epsilon = 10

    implicit val degugOn = true
    val WKTINPUT = false
    val buffer = Source.fromFile(input)
    val points = timer{"Reading points"}{
      buffer.getLines.zipWithIndex.toList
        .map{ line =>
          if(WKTINPUT){
            val id = line._2
            val point = reader.read(line._1).asInstanceOf[Point]

            point.setUserData(id)
            point
          } else {
            val arr = line._1.split("\t")
            val id = arr(0).toInt
            val x = arr(1).toDouble
            val y = arr(2).toDouble
            val point = geofactory.createPoint(new Coordinate(x, y))

            point.setUserData(id)
            point
          }
        }
    }
    buffer.close

    val pairs = timer{"Getting pairs"}{
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

    val cliques = timer{"Getting maximal cliques"}{
      val graph = new SimpleGraph[Geometry, DefaultEdge](classOf[DefaultEdge])
      points.foreach{ vertex =>  graph.addVertex(vertex) }
      pairs.foreach{ case(a, b) => graph.addEdge(a, b) }
      val cliques= {
        val finder = new BronKerboschCliqueFinder(graph)
        finder.iterator.asScala.toList
      }
      cliques
    }

    debug{
      save{"/tmp/edgesPoints.wkt"}{
        points.map{ point =>
          val wkt = point.toText()
          val id = point.getUserData.asInstanceOf[Int]
          s"$wkt\t$id\n"
        }
      }
      save{"/tmp/edgesPairs.wkt"}{
        pairs.map{ case(a, b) =>
          val coords = Array(a.getCoordinate, b.getCoordinate)
          val line = geofactory.createLineString(coords)
          val wkt = line.toText()
          val id1 = a.getUserData.asInstanceOf[Int]
          val id2 = b.getUserData.asInstanceOf[Int]

          s"$wkt\t$id1\t$id2\n"
        }
      }
      save{"/tmp/edgesCliques.wkt"}{
        cliques.map{ clique =>
          val vertices = geofactory.createMultiPoint(
            clique.asScala.toArray.map(_.getCoordinate)
          )
          val convex = vertices.convexHull
          val mbc = new MinimumBoundingCircle(convex)
          val centroid = geofactory.createPoint(mbc.getCentre)
          val wkt1 = convex.toText
          val wkt2 = centroid.toText
          val radius = mbc.getRadius
          val n = clique.size
          
          s"$wkt1\t$wkt2\t$radius\t$n\n"
        }
      }
      save{"/tmp/edgesCentroids.wkt"}{
        cliques.map{ clique =>
          val vertices = geofactory.createMultiPoint(
            clique.asScala.toArray.map(_.getCoordinate)
          )
          val convex = vertices.convexHull
          val mbc = new MinimumBoundingCircle(convex)
          val centroid = geofactory.createPoint(mbc.getCentre)
          val radius = mbc.getRadius
          val wkt = centroid.buffer(radius, 15).toText
          val n = clique.size
          
          s"$wkt\t$radius\t$n\n"
        }
      }
    }
  }

  def clocktime: Long = System.nanoTime()

  def timer[R](msg: String)(block: => R): R = {
    val t0 = clocktime
    val result = block    // call-by-name
    val t1 = clocktime
    println("%-30s|%6.2f".format(msg, (t1 - t0) / 1e9))
    result
  }

  def debug[R](block: => R)(implicit d: Boolean): Unit = { if(d) block }

    def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1e9)
    println(s"Saved ${filename} in ${time}s [${content.size} records].")
  }
}
