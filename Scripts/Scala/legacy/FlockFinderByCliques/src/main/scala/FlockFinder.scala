package edu.ucr.dblab

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.geom.{Coordinate, Point}
import com.vividsolutions.jts.io.WKTReader
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.alg.clique.BronKerboschCliqueFinder
import org.apache.commons.math3.geometry.euclidean.twod.DiskGenerator
import scala.io.Source
import scala.collection.JavaConverters._
import java.io.PrintWriter

import edu.ucr.dblab.CliqueFinderUtils._

object FlockFinder {
  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)
    implicit val tolerance = Tolerance(1.0 / model.getScale)
    implicit val generator = new DiskGenerator
    val reader = new WKTReader()
    val input = args(0)
    val distance = args(1).toDouble
    val epsilon = distance + tolerance.value
    val r = (distance / 2.0) + tolerance.value
    val r2 = math.pow(distance / 2.0, 2) + tolerance.value
    val mu = args(2).toInt

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
    println(s"Pairs: ${pairs.size}")

    val cliques = timer{"Getting maximal cliques"}{
      val graph = new SimpleGraph[Geometry, DefaultEdge](classOf[DefaultEdge])
      points.foreach{ vertex =>  graph.addVertex(vertex) }
      pairs.foreach{ case(a, b) => graph.addEdge(a, b) }
      val cliques = {
        val finder = new PBKCliqueFinder(graph)
        finder.iterator.asScala.toList.map{
          _.asScala.toList.map(_.asInstanceOf[Point])
        }
      }
      cliques.zipWithIndex
    }.map{ case(points, id) => Clique(id, points)}
      .filter(_.points.size >= mu)

    val disks = timer{"Getting centers"}{
      val mbcs = cliques.map{ clique =>
        val mbc = getMBC(clique.points)
        (mbc, clique)
      }

      val centersA = mbcs.filter{ case(mbc, clique) =>
        mbc.radius <= r
      }.map{ case(mbc, clique) =>
          val center = mbc.center
          val pids = clique.points.map(_.getUserData.asInstanceOf[Int]).sorted

          Disk(center.getX, center.getY, pids)
      }

      val centersB = mbcs.filter{ case(mbc, clique) =>
        mbc.radius > r
      }.flatMap{ case(mbc, clique) =>
          val points = clique.points
          val centers = findCenters(points, epsilon, r2).distinct

          println(s"Centers: ${centers.size}")
          
          val disks = centers.map{ center =>
            val pids = points.filter(_.distance(center) <= r)
              .map(_.getUserData.asInstanceOf[Int]).sorted
            Disk(center.getX, center.getY, pids)
          }
          disks
          //pruneDisks(disks, mu)
      }

      centersA ++ centersB
    }.distinct
    println(s"Disks: ${disks.size}")

    /*
    val x = center.getX
    val y = center.getY
    val pids = clique.points.map{ point =>
      point.getUserData.asInstanceOf[Int]
    }.toList.sorted
    Disk(x, y, pids)
    
    val disks = {
      for {
        p <- points
        c <- centers if c.distance(p) <= r
      } yield (c, p)
    }.groupBy(_._1).map{ case(center, pairs) =>
        val x = center.getX
        val y = center.getY
        val points = pairs.map(_._2)
        val pids = points.map(_.getUserData.asInstanceOf[Int]).sorted
        Disk(x, y, pids)
    }.toList

    pruneDisks(disksA union disksB, mu)
    pruneDisks(disks, mu).map(disk => disk.copy(clique_id = clique.id))
  }.flatten
     */

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

      /*
      save{"/tmp/edgesDisks.wkt"}{
        disks.map{ disk =>
          val coord = new Coordinate(disk.x, disk.y)
          val centroid = geofactory.createPoint(coord)
          val radius = epsilon / 2.0
          val wkt = centroid.buffer(radius, 15).toText
          val pids = disk.pids.mkString(" ")
          
          s"$wkt\t$pids\n"
        }
      }
       */

      save{s"/tmp/PFLOCK_E${distance.toInt}_M${mu}.txt"}{
        disks.map{ disk =>
          val pids = disk.pids.sorted.mkString(" ")
          
          s"0, 0, $pids\n"
        }
      }

      val C = cliques.filter{ clique =>
        val mbc = getMBC(clique.points)

        mbc.radius > r
      }
      save{"/tmp/edgesCliques.wkt"}{
        C.map{ clique =>
          val id = clique.id
          val vertices = geofactory.createMultiPoint(
            clique.points.toArray.map(_.getCoordinate)
          )
          val convex = vertices.convexHull
          val wkt = convex.toText
          
          s"$wkt\t$id\n"
        }
      }
      save{"/tmp/edgesPCliques.wkt"}{
        C.map{ clique =>
          val id = clique.id
          val vertices = geofactory.createMultiPoint(
            clique.points.toArray.map(_.getCoordinate)
          )
          val wkt = vertices.toText()
          
          s"$wkt\t$id\n"
        }
      }
      save{"/tmp/edgesCentres.wkt"}{
        C.map{ clique =>
          val id = clique.id
          val mbc = getMBC(clique.points)
          val wkt = mbc.center.toText
          
          s"$wkt\t$id\n"
        }
      }
      save{"/tmp/edgesCircles.wkt"}{
        C.map{ clique =>
          val id = clique.id
          val mbc = getMBC(clique.points)
          val wkt = mbc.center.buffer(mbc.radius, 25).toText
          
          s"$wkt\t$id\n"
        }
      }
      save{"/tmp/edgesExtremes.wkt"}{
        C.map{ clique =>
          val id = clique.id
          val mbc = getMBC(clique.points)
          val wkt = geofactory.createMultiPoint(mbc.extremes).toText()
          
          s"$wkt\t$id\n"
        }
      }
    }
  }
}
