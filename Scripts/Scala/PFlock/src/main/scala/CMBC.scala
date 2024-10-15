package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.pbk.PBK.bk
import edu.ucr.dblab.pflock.welzl.Welzl
import org.apache.commons.geometry.enclosing.EnclosingBall
import org.apache.commons.geometry.euclidean.twod.Vector2D
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, PrecisionModel}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter

object CMBC {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

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
    maximals.map{ maximal =>
      val wkt = maximal.center.toText
      val pids = maximal.pidsText
      s"$wkt\t$pids\n"
    }.foreach(print)

    val vertices = points.map{_.point}
    val edges = getEdges(points)
    log(s"Reading data|START")
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
    val cliques = bk(vertices, edges).iterator.filter(_.size >= S.mu).toList.zipWithIndex.map{ case(clique, id) => Clique(clique, id)}
    if(S.debug){
      cliques.foreach{ clique =>
        val id = clique.id
        save(s"/tmp/sample${id}.tsv"){ clique.text }
      }
    }

    def getCircleMBC(mbc: EnclosingBall[Vector2D]): String = {
      val center = G.createPoint(new Coordinate(mbc.getCenter.getX, mbc.getCenter.getY))
      center.buffer(mbc.getRadius, 25).toText
    }

    def getEpsilonMBC(cliques: List[Clique], r: List[Point]): List[Point] = {
      cliques match {
        case Nil => r
        case clique :: tail =>
          val mbc = Welzl.mbc(clique.points)
          if(mbc.getRadius <= S.r){
            //println("END")
            println(s"${getCircleMBC(mbc)}\t${mbc.getRadius}")
            val center = G.createPoint(new Coordinate(mbc.getCenter.getX, mbc.getCenter.getY))
            center.setUserData(mbc.getRadius)
            //getEpsilonMBC(tail, r :+ center)
            r
          } else {
            val x = mbc.getSupport.asScala.map { v =>
              G.createPoint(new Coordinate(v.getX, v.getY))
            }.minBy{ p => (p.getX, p.getY)}
            val P = clique.points.filterNot{p => math.abs(p.getX - x.getX) < S.tolerance & math.abs(p.getY - x.getY) < S.tolerance}
            val clique_prime = Clique(P, clique.id)
            getEpsilonMBC(clique_prime +: tail, r)
          }
      }
    }

    val mbcs = getEpsilonMBC(cliques, List.empty[Point])

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
          val radius = round(mbc.getUserData.asInstanceOf[Double])
          val wkt = mbc.buffer(radius, 25).toText
          s"$wkt\t$radius\n"
        }
      }
    }

    log(s"Done.|END")
  }
}
