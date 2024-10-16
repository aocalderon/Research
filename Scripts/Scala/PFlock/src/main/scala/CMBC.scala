package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.pbk.PBK.bk
import edu.ucr.dblab.pflock.welzl.Welzl
import org.apache.commons.geometry.enclosing.EnclosingBall
import org.apache.commons.geometry.euclidean.twod.Vector2D
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon, PrecisionModel}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters.asScalaBufferConverter

object CMBC {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  case class Data(id: Int, radius: Double)
  case class Clique(points: List[Point], id: Int){
    val length = points.length
    def text: List[String] = {
      points.zipWithIndex.map{ case(point, id) =>
        val x = point.getX
        val y = point.getY
        val t = 0
        s"$id\t$x\t$y\t$t\n"
      }
    }
  }

  def main(args: Array[String]): Unit = {
    //generateData(10000, 1000, 1000, "/home/acald013/Research/Datasets/P10K_W1K_H1K.tsv")
    implicit val params = new BFEParams(args)

    implicit val S = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "CMBC",
      capacity = params.capacity(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug()
    )
    implicit val G = new GeometryFactory(new PrecisionModel(S.scale))

    val points = readPoints(params.dataset())
    val (maximals, stats) = PSI.run(points)
    stats.printPSI()

    val vertices = points.map{_.point}
    val edges = getEdges(points)
    log(s"Reading data|START")
    val cliques = bk(vertices, edges).iterator.filter(_.size >= S.mu).toList.zipWithIndex.map{ case(clique, id) => Clique(clique, id)}
    if(S.debug){
      cliques.foreach{ clique =>
        val id = clique.id
        //save(s"/tmp/sample${id}.tsv"){ clique.text }
      }
    }


    val embcs = getEpsilonMBCs(cliques, List.empty[Point])
    val mbcs  = getMBCs(cliques, List.empty[Point])
    val chs   = getConvexHulls(cliques, List.empty[Polygon])

    val mbcs_map = mbcs.filter{ mbc =>
      val r = mbc.getUserData.asInstanceOf[Data].radius
      r > S.r
    }.map{ mbc =>
      val id = mbc.getUserData.asInstanceOf[Data].id
      id -> mbc
    }.toMap


    /*
  mbcs_map.keys.map{ i =>
    val path = s"/tmp/sample${i}.tsv"
    val points = readPoints(path)
    val (maximals1, stats1) = PSI.run(points)
    val n1 = maximals1.length
    val mbc = mbcs_map(i)
    val (maximals2, stats2) = PSI.runByPivot(points, mbc)
    val n2 = maximals2.length
    Checker.checkMaximalDisks(maximals1, maximals2, "PSI", "PSI_Pivot", points)

    s"$i\t$n1\t$n2\t${n2-n1}"
  }.foreach{println}

    .groupBy(_._1).map{ case(id, dists_prime) =>
    val dists = dists_prime.map(_._2).toList
    val min = dists.min
    val max = dists.max
    val all = dists.sorted.mkString(" ")

    s"$id\t$min\t$max\t$all"
  }.foreach{println}
*/


    debug{
      save("/tmp/edgesCliques.wkt") {
        cliques.map { clique =>
          val wkt = G.createMultiPoint(clique.points.toArray).toText
          val id = clique.id
          s"$wkt\t$id\n"
        }
      }
      save("/tmp/edgesMBC.wkt") {
        mbcs.map { mbc =>
          val radius = round(mbc.getUserData.asInstanceOf[Data].radius)
          val wkt = mbc.buffer(radius, 25).toText
          s"$wkt\t$radius\n"
        }
      }
      save("/tmp/edgesEMBC.wkt") {
        embcs.map { embc =>
          val radius = round(embc.getUserData.asInstanceOf[Data].radius)
          val wkt = embc.buffer(radius, 25).toText
          s"$wkt\t$radius\n"
        }
      }
      save("/tmp/edgesCH.wkt") {
        chs.map { ch =>
          val wkt = ch.toText
          val n   = ch.getCoordinates.length
          s"$wkt\t$n\n"
        }
      }
      save("/tmp/edgesMD.wkt") {
        maximals.map { maximal =>
          val wkt  = maximal.getCircleWTK
          val pids = maximal.pidsText
          s"$wkt\t$pids\n"
        }
      }
      save("/tmp/edgesMC.wkt") {
        maximals.map { maximal =>
          val wkt  = maximal.center.toText
          val pids = maximal.pidsText
          s"$wkt\t$pids\n"
        }
      }
    }

    log(s"Done.|END")
  }

  def getCircleMBC(mbc: EnclosingBall[Vector2D])(implicit G: GeometryFactory): String = {
    val center = G.createPoint(new Coordinate(mbc.getCenter.getX, mbc.getCenter.getY))
    center.buffer(mbc.getRadius, 25).toText
  }

  @tailrec
  def getEpsilonMBCs(cliques: List[Clique], r: List[Point])(implicit G: GeometryFactory, S: Settings): List[Point] = {
    cliques match {
      case Nil => r
      case clique :: tail =>
        val mbc = Welzl.mbc(clique.points)
        if(mbc.getRadius <= S.r){
          val center = G.createPoint(new Coordinate(mbc.getCenter.getX, mbc.getCenter.getY))
          center.setUserData( Data(clique.id, mbc.getRadius))
          getEpsilonMBCs(tail, r :+ center)
        } else {
          val x = mbc.getSupport.asScala.map { v =>
            G.createPoint(new Coordinate(v.getX, v.getY))
          }.minBy{ p => (p.getX, p.getY)}
          val P = clique.points.filterNot{p => math.abs(p.getX - x.getX) < S.tolerance & math.abs(p.getY - x.getY) < S.tolerance}
          val clique_prime = Clique(P, clique.id)
          getEpsilonMBCs(clique_prime +: tail, r)
        }
    }
  }

  @tailrec
  def getMBCs(cliques: List[Clique], r: List[Point])(implicit G: GeometryFactory, S: Settings): List[Point] = {
    cliques match {
      case Nil => r
      case clique :: tail =>
        val mbc = Welzl.mbc(clique.points)
        val center = G.createPoint(new Coordinate(mbc.getCenter.getX, mbc.getCenter.getY))
        center.setUserData( Data(clique.id, mbc.getRadius))
        getMBCs(tail, r :+ center)
    }
  }

  @tailrec
  def getConvexHulls(cliques: List[Clique], r: List[Polygon])(implicit G: GeometryFactory, S: Settings): List[Polygon] = {
    cliques match {
      case Nil => r
      case clique :: tail =>
        val geom = G.createMultiPoint(clique.points.toArray)
        val ch = geom.convexHull().asInstanceOf[Polygon]
        getConvexHulls(tail, r :+ ch)
    }
  }
}
