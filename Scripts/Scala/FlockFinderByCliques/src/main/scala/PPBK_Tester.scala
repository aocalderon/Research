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
    val r  = (distance / 2.0) + tolerance.value
    val r2 = math.pow(distance / 2.0, 2) + tolerance.value
    val mu = args(2).toInt
    implicit val sortMode = SortMode(args(3).toInt)
    
    val (vertices, edges) = readTrajs(filename, epsilon)

    vertices.foreach(graph.addVertex)
    edges.foreach{ case(a, b) => graph.addEdge(a, b) }

    implicit val Rs = new PointPrefixTree
    var R = HashSet[Point]()
    var P = HashSet[Point]()
    var X = HashSet[Point]()

    graph.vertexSet.asScala.foreach{ v => P.add(v)}

    IK_*(R, P, X)

    
    println(Rs.root.printTree)    
    println("getNextWithBranches:")
    val first = Rs.root.getNextWithBranches
    println(s"${first.toText}")
    first.updateDisks(epsilon, r2, mu)
    first.parent.printNodes
    first.updateDisksFromParent(epsilon, mu)
    println(s"${first.toText}")
    first.printLeaves
    

    //
    val cliques = Rs.transactions.zipWithIndex.map{ case(r, id) =>
      Clique(id, r._1)
    }.toList

    cliques.map{ clique =>
      val pts = clique.points
      convexHull(pts).map(_.getUserData.asInstanceOf[Int]).mkString(" ")
    }

    val points = cliques.map{ clique =>
      clique.points.map{ point =>
        val wkt = point.toText
        val pid = point.getUserData
        val cid = clique.id
        s"$wkt\t$pid\t$cid\n"
      }
    }.flatten.toList
    save("/tmp/edgesPoints.wkt"){ points }

    save("/tmp/edgesCliques.wkt"){
      cliques.zipWithIndex.map{ case(clique, id) =>
        val pts = clique.points
        val coords = convexHull(pts).map(_.getCoordinate).toArray
        val wkt = geofactory.createPolygon(coords).toText

        s"$wkt\t$id\n"
      }
    }

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
      }.filter(_.pids.size >= mu)
    }

    //
    save("test.dot"){
      "digraph G {\n" +: Rs.root.toDot :+ "}"
    }

    //
    //println("Points per clique:")
    //Rs.transactions.map{t=> t._1.map(_.getUserData).mkString(" ")}.foreach{println}

    save("/tmp/edgesCenters.wkt"){
      disks.map{ disks =>
        disks.map{ disk =>
          val center = geofactory.createPoint(new Coordinate(disk.x, disk.y))
          val wkt = center.toText
          val pids = disk.pids.mkString(" ")
          s"$wkt\t$pids\t${disk.clique_id}\n"
        }
      }.flatten
    }
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
      val id = disks.head.clique_id
      pruneDisks(disks.toList, mu).map{ d => d.copy(clique_id = id)}
    }

    save("/tmp/edgesMaximals.wkt"){
      maximals.map{ mpc =>
        mpc.map{ maximal =>
          val center = geofactory.createPoint(new Coordinate(maximal.x, maximal.y))
          val wkt = center.toText
          val pids = maximal.pids.mkString(" ")
          val id = maximal.clique_id

          s"$wkt\t$pids\t$id\n"
        }
      }.flatten
    }

    save("/tmp/flocks.txt"){
      maximals.map(_.map(_.pids.mkString(" ")).mkString("\n") + "\n")
    }

  }
}
