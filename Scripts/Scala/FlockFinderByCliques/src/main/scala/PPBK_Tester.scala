package edu.ucr.dblab

import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.Graphs
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Point, Coordinate}
import scala.collection.mutable.{ListBuffer, HashSet}
import scala.collection.JavaConverters._

import java.io.PrintWriter
import scala.io.Source

object PPBK_Tester {
  import CliqueFinderUtils._
  import PPBK._

  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)
    implicit val tolerance = Tolerance(1.0 / model.getScale)
    implicit val graph = new SimpleGraph[Point, DefaultEdge](classOf[DefaultEdge])

    val filename = args(0)
    val distance = args(1).toDouble
    val epsilon = distance + tolerance.value
    val r = (distance / 2.0) + tolerance.value
    val r2 = math.pow(distance / 2.0, 2) + tolerance.value
    val mu = args(2).toInt
    implicit val sortMode = SortMode(args(3).toInt)
    
    val (vertices, edges) = readTrajs(filename, epsilon)

    vertices.foreach(graph.addVertex)
    edges.foreach{ case(a, b) => graph.addEdge(a, b) }

    implicit val Rs = new FPTree[Point]
    var R = HashSet[Point]()
    var P = HashSet[Point]()
    var X = HashSet[Point]()

    graph.vertexSet.asScala.foreach{ v => P.add(v)}

    IK_*(R, P, X)

    val cliques = Rs.transactions.zipWithIndex.map{ case(r, id) =>
      Clique(id, r._1)
    }.toList

    val points = cliques.map{ clique =>
      clique.points.map{ point =>
        val wkt = point.toText
        val id = clique.id
        s"$wkt\t$id\n"
      }
    }.flatten.toList

    //
    save("/tmp/edgesPoints.wkt"){ points }

    val disks = cliques.map{ clique =>
      val points  = clique.points
      val centers = findCenters(points, epsilon, r2)
      val join = for {
        p <- points
        c <- centers if c.distance(p) <= r
      } yield {
        (c, p.getUserData.asInstanceOf[Int])
      }
      join.groupBy(_._1).mapValues(_.map(_._2)).map{ case(center, pids) =>
        Disk(center.getX, center.getY, pids.sorted, clique.id)
      }
    }

    //
    save("/tmp/edgesDisks.wkt"){
      disks.map{ disks =>
        disks.map{ disk =>
          val center = geofactory.createPoint(new Coordinate(disk.x, disk.y))
          val wkt = center.buffer(r, 15).toText
          val pids = disk.pids.mkString(" ")
          s"$wkt\t$pids\t${disk.clique_id}\n"
        }
      }.flatten
    }

    val maximals = disks.map{ disks =>
      pruneDisks(disks.toList, mu)
    }

    maximals.foreach{println}

  }
  
}
