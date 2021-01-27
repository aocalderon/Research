package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.geom.{Coordinate, Point}
import com.vividsolutions.jts.io.WKTReader
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.Graphs
import scala.collection.JavaConverters._
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer

import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.FF._

import edu.ucr.dblab.pflock.pbk.PBK_Utils._
import edu.ucr.dblab.pflock.pbk.PBK.bk

import edu.ucr.dblab.pflock.welzl.Welzl2

object FF_prime {

  def main(args: Array[String]): Unit = {
    implicit val params = new Params(args)
    implicit val geofactory = new GeometryFactory(new PrecisionModel(params.scale()))
    implicit val tolerance = Tolerance(1.0 / geofactory.getPrecisionModel.getScale)
    implicit val degugOn = params.debug()

    val input = params.input()
    val epsilon_prime = params.epsilon()
    val epsilon = epsilon_prime + tolerance.value
    val r = (epsilon_prime / 2.0) + tolerance.value
    val r2 = math.pow(epsilon_prime / 2.0, 2) + tolerance.value
    val mu = params.mu()

    val (vertices, edges) = readTrajs(input, epsilon)
    val cliques = bk(vertices, edges)

    val welzl = Welzl2.encloser
    val (maximals_prime, disks_prime) = cliques.iterator//.toList.par
      .filter(_.size >= mu)                // Filter cliques with not enough members...
      .map{ points =>
        val mbc = welzl.enclose(Welzl2.asVectors(points))
        (mbc, points)
      }
      .partition{ case(mbc, points) =>     // Split cliques enclosed by a single disk...
        mbc.getRadius <= r 
      }
    val maximals1 = maximals_prime.map{ case(mbc, points) =>
      val pids = points.map(_.getUserData.asInstanceOf[Int])
      val center = geofactory.createPoint(
        new Coordinate(mbc.getCenter.getX, mbc.getCenter.getY)
      )

      Disk(center, pids, List.empty)
    }.toList

    val maximals2 = disks_prime.map{ case(mbc, points) =>
      val centers = computeCenters(computePairs(points, epsilon), r2)
      val disks = getDisks(points, centers, r, mu)
      pruneDisks(disks)
    }.toList.flatten

    val maximals = pruneDisks(maximals1 ++ maximals2)

    println(s"Maximals: ${maximals.size}")

    debug{
      save("/tmp/edgesCliques.wkt"){
        toWKT(cliques).map(_.toText + "\n").toList
      }

      val n = maximals.size
      save(s"/tmp/edgesMaximals${n}.wkt"){
        maximals.map{ disk =>
          val wkt  = disk.center.toText
          val pids = disk.pids.mkString(" ")

          s"$wkt\t$pids\n"
        }.toList
      }
    }
  }
}
