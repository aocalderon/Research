package edu.ucr.dblab

import com.vividsolutions.jts.algorithm.MinimumBoundingCircle
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.geom.{Coordinate, Point}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.index.quadtree.Quadtree
import scala.collection.JavaConverters._
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.alg.clique.BronKerboschCliqueFinder
import java.io.PrintWriter
import scala.io.Source
import scala.annotation.tailrec
import edu.ucr.dblab.djoin.SPMF._

import edu.ucr.dblab.CliqueFinderUtils._

object FlockFinder {
  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)
    val reader = new WKTReader()
    val precision = 0.001
    val input = args(0)
    val distance = args(1).toDouble
    val epsilon = distance + precision
    val r = (distance / 2.0) + precision
    val r2 = math.pow(distance / 2.0, 2) + precision
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

    val disks = timer{"Getting disks"}{
      // Finding cliques which minimum bounding clircle (mbc) is less than epsilon...
      val mbcs = cliques.map{ clique =>
        val vertices = geofactory.createMultiPoint(
          clique.points.toArray.map(_.getCoordinate)
        )

        /*****/
        import org.apache.commons.math3.geometry.euclidean.twod.DiskGenerator
        import org.apache.commons.math3.geometry.euclidean.twod.Vector2D
        import org.apache.commons.math3.geometry.enclosing.SupportBallGenerator
        val diskGenerator = new DiskGenerator
        val welzl = new Welzl(1.0 / model.getScale, diskGenerator)
        val pIt = clique.points.map{ p => new Vector2D(p.getX, p.getY)}.toIterable
        val sec = welzl.enclose(pIt.asJava)
        println(s"The SEC support: ${sec.getSupport.map(_.toString).mkString(" ")}")
        /*****/

        val mbc = new MinimumBoundingCircle(vertices)
        (mbc, clique)
      }

      val disksA = mbcs.filter{ case(mbc, clique) =>
        mbc.getRadius <= r
      }.map{ case(mbc, clique) =>
          val center = mbc.getCentre
          val x = center.x
          val y = center.y
          val pids = clique.points.map{ point =>
            point.getUserData.asInstanceOf[Int]
          }.toList.sorted
          Disk(x, y, pids)
      }

      val disksB = mbcs.filter{ case(mbc, clique) =>
        mbc.getRadius > r
      }.map{ case(mbc, clique) =>
          val points = clique.points

          val centers = findCenters(points, epsilon, r2).distinct

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

          pruneDisks(disks, mu).map(disk => disk.copy(clique_id = clique.id))
      }.flatten

      val disks = pruneDisks(disksA union disksB, mu)

      /**********************************************************************/
      def getMBCs(points: List[Point]): List[MBC] = {
        @tailrec
        def getMBCsTailrec(points: List[Point], MBCs: List[MBC]): List[MBC] = {

          val coords = points.map(_.getCoordinate).toArray
          val circle = new MinimumBoundingCircle(geofactory.createMultiPoint(coords))
          val extremals = circle.getExtremalPoints
          val inner = points.map(_.getUserData.asInstanceOf[Int])
          val outer = points.filter{ point =>
            extremals.contains(point.getCoordinate)
          }.map(_.getUserData.asInstanceOf[Int])
          val mbc = MBC(circle, inner, outer)
          if(circle.getRadius <= r){
            MBCs :+ mbc
          } else {
            val new_points = points.filter{ point =>
              !extremals.contains(point.getCoordinate)
            }
            getMBCsTailrec(new_points, MBCs :+ mbc)
          }
        }
        getMBCsTailrec(points, List.empty[MBC])
      }
      val mbcs2 = cliques.flatMap{ clique =>
        getMBCs(clique.points).zipWithIndex.map{ case(mbc, order) =>
          (clique.id, order, mbc)
        }
      }
      val pointsMap = points.map{ point =>
        val id = point.getUserData.asInstanceOf[Int]
        id -> point
      }.toMap
      save{"/tmp/sampleInners.tsv"}{
        val convex = cliques.flatMap{ clique =>
          val pts = getMBCs(clique.points).map(_.inner).last
            .map{ id => pointsMap(id) }
          geofactory.createMultiPoint(pts.toArray)
            .convexHull().getCoordinates
        }
        val pMap = points.map{ p => p.getCoordinate -> p}.toMap
        convex.map{ coord =>
          val point = pMap(coord)
          val id = point.getUserData.asInstanceOf[Int]
          val x = point.getX
          val y = point.getY
          s"$id\t$x\t$y\t0\n"
        }
      }
      save{"/tmp/sampleOuters.tsv"}{
        cliques.flatMap{ clique =>
          getMBCs(clique.points).reverse.tail.flatMap(_.outer)
            .map{ id =>
              val point = pointsMap(id)
              val x = point.getX
              val y = point.getY
              s"$id\t$x\t$y\t0\n"
            }
        }
      }
      save{"/tmp/edgesMBCs.wkt"}{
        mbcs2.map{ case(id, order, mbc) =>
          val wkt = mbc.circle.getCircle.toText()
          val inner = mbc.inner.mkString(" ")
          val outer = mbc.outer.mkString(" ")
          s"$wkt\t$id\t$order\t$outer\t$inner\n"
        }
      }

      save{"/tmp/edgesDisksB.wkt"}{
        val d = disksB.map{ disk => (disk.clique_id, disk)}.groupBy(_._1)
          .filter{ case(id, disks) =>
            disks.length > 3
          }
        d.mapValues(_.map(_._2)).values.flatten.map{ disk =>
          val id = disk.clique_id
          val coord = new Coordinate(disk.x, disk.y)
          val centroid = geofactory.createPoint(coord)
          val radius = epsilon / 2.0
          val wkt = centroid.buffer(radius, 15).toText
          val pids = disk.pids.mkString(" ")
          
          s"$wkt\t$id\t$pids\n"
        }.toList
      }
      /**********************************************************************/

      disks
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
      save{"/tmp/pflock.txt"}{
        disks.map{ disk =>
          val pids = disk.pids.sorted.mkString(" ")
          
          s" $pids\n"
        }
      }

      val C = cliques.filter{ clique =>
        val vertices = geofactory.createMultiPoint(
          clique.points.toArray.map(_.getCoordinate)
        )
        val convex = vertices.convexHull
        val mbc = new MinimumBoundingCircle(convex)

        mbc.getRadius > r
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
          val vertices = geofactory.createMultiPoint(
            clique.points.toArray.map(_.getCoordinate)
          )
          val mbc = new MinimumBoundingCircle(vertices)
          val wkt = geofactory.createPoint(mbc.getCentre).toText
          
          s"$wkt\t$id\n"
        }
      }
      save{"/tmp/edgesCircles.wkt"}{
        C.map{ clique =>
          val id = clique.id
          val vertices = geofactory.createMultiPoint(
            clique.points.toArray.map(_.getCoordinate)
          )
          val mbc = new MinimumBoundingCircle(vertices)
          val wkt = mbc.getCircle.toText
          
          s"$wkt\t$id\n"
        }
      }
      save{"/tmp/edgesExtremes.wkt"}{
        C.map{ clique =>
          val id = clique.id
          val vertices = geofactory.createMultiPoint(
            clique.points.toArray.map(_.getCoordinate)
          )
          val mbc = new MinimumBoundingCircle(vertices)
          val wkt = geofactory.createMultiPoint(mbc.getExtremalPoints).toText()
          
          s"$wkt\t$id\n"
        }
      }
      save{"/tmp/edgesDiameters.wkt"}{
        C.map{ clique =>
          val id = clique.id
          val vertices = geofactory.createMultiPoint(
            clique.points.toArray.map(_.getCoordinate)
          )
          val mbc = new MinimumBoundingCircle(vertices)
          val pts = closestPoints(mbc.getExtremalPoints)
          val wkt = geofactory.createLineString(pts).toText()
          
          s"$wkt\t$id\n"
        }
      }
      
    }
  }
}
