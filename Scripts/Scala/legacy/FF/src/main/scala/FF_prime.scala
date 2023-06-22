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
    implicit val settings = Settings(params.epsilon(), params.mu(),
      tolerance = params.tolerance(),
      debug = params.debug()
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    val input = params.input()
    println(s"Scale:    \t${settings.scale}")
    println(s"Tolerance:\t${settings.tolerance}")
    println(s"Epsilon:  \t${settings.epsilon}")
    println(s"r:        \t${settings.r}")
    println(s"Mu:       \t${settings.mu}")

    val (vertices, edges) = readTrajs(input, settings.epsilon)
    val cliques = bk(vertices, edges)

    def round(x: Double)(implicit settings: Settings): Double = {
      val decimal_positions = math.log10(settings.scale).toInt
      BigDecimal(x).setScale(decimal_positions, BigDecimal.RoundingMode.HALF_UP).toDouble
    }
    val welzl = Welzl2.encloser
    val (maximals_prime, disks_prime) = cliques.iterator
      .filter(_.size >= settings.mu)        // Filter cliques with not enough members...
      .map{ points =>
        val mbc = welzl.enclose(Welzl2.asVectors(points))
        (mbc, points)
      }
      .partition{ case(mbc, points) =>     // Split cliques enclosed by a single disk...
        round(mbc.getRadius) < settings.r 
      }
    val maximals1 = maximals_prime.map{ case(mbc, points) =>
      val pids = points.map(_.getUserData.asInstanceOf[Int])
      val center = geofactory.createPoint(
        new Coordinate(mbc.getCenter.getX, mbc.getCenter.getY)
      )

      Disk(center, pids, List.empty)
    }.toList

    val maximals2 = disks_prime.map{ case(mbc, points) =>
      val centers = computeCenters(computePairs(points, settings.epsilon), settings.r2)
      val disks = getDisks(points, centers, settings.r, settings.mu)
      pruneDisks(disks)
    }.toList.flatten

    val maximals = pruneDisks(maximals1 ++ maximals2)

    println(s"Maximals: ${maximals.size}")

    if(settings.debug){
      save("/tmp/edgesCliques.wkt"){
        toPolygons(cliques, settings.mu).map{ poly =>
          val wkt = poly.toText
          val pids = poly.getUserData.asInstanceOf[List[Int]].sorted.mkString(" ")

          s"$wkt\t$pids\n"
        }.toList
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
