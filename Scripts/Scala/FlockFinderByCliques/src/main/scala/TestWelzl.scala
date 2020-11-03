package edu.ucr.dblab

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.geom.{Coordinate, Point}
import com.vividsolutions.jts.io.WKTReader
import org.apache.commons.math3.geometry.euclidean.twod.DiskGenerator
import org.apache.commons.math3.geometry.euclidean.twod.Vector2D
import org.apache.commons.math3.geometry.enclosing.SupportBallGenerator
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.alg.clique.BronKerboschCliqueFinder
import scala.annotation.tailrec
import scala.io.Source
import scala.collection.JavaConverters._
import java.io.PrintWriter
import edu.ucr.dblab.CliqueFinderUtils._

object TestWelzl {
  case class MBCByRadius(mbc: MBC, in: List[Point], out: List[Point]) {
    override def toString = {
      val wkt = mbc.center.toText
      val ins  = in.map(_.getUserData.asInstanceOf[Int]).mkString(" ")
      val outs = out.map(_.getUserData.asInstanceOf[Int]).mkString(" ")
      s"$wkt\t|$ins\t|$outs"
    }
  }

  def getMBCByRadius(points: List[Point], r: Double)
    (implicit geofactory: GeometryFactory,
      diskGenerator: DiskGenerator, tolerance: Tolerance): MBCByRadius = {

    val welzl = new Welzl(tolerance.value, diskGenerator)
    val pIt = points.map{new Point2D(_)}.toIterable

    val rmbc = welzl.getMBCByRadius((pIt: Iterable[Vector2D]).asJava, r)
    val vCenter = rmbc.getCenter
    val center = geofactory.createPoint(new Coordinate(vCenter.getX, vCenter.getY))
    val radius = rmbc.getRadius
    val extremes = rmbc.getSupport.map{_.asInstanceOf[Point2D].point}
    val mbc = MBC(center, radius, extremes)
    val (in, out) = points.partition(_.distance(center) <= r)

    MBCByRadius(mbc, in, out)
  }

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
    println(s"Cliques: ${cliques.size}")

    val cliques2 = timer{"Getting cliques > e"}{
      val mbcs = cliques.map{ clique =>
        val mbc = getMBC(clique.points)
        (mbc, clique)
      }

      mbcs.filter{ case(mbc, clique) =>
        mbc.radius > r
      }.map{ case(mbc, clique) => clique }
     
    }
    println(s"Cliques2: ${cliques2.size}")

    val disks2 = timer{"Disks alternative 2"}{
      cliques2.map{ clique =>
        val mbcr = getMBCByRadius(clique.points, r)
        val x = mbcr.mbc.center.getX
        val y = mbcr.mbc.center.getY
        val pids = mbcr.in.map(_.getUserData.asInstanceOf[Int]).sorted

        val disk = Disk(x, y, pids)
        (clique, disk)
      }
    }

    val disks1 = timer{"Disks alternative 1"}{
      cliques2.map{ clique =>
        val centers = findCenters(clique.points, epsilon, r2)
        val disks_prime = centers.map{ center =>
          val pids = clique.points.filter(_.distance(center) <= epsilon)
            .map(_.getUserData.asInstanceOf[Int]).sorted
          Disk(center.getX, center.getY, pids)
        }
        val disks = pruneDisks(disks_prime, mu)
        (clique, disks)
      }
    }

  }
}
