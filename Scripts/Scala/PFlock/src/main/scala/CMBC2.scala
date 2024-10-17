package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.PSI.insertDisk
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.pbk.PBK.bk
import edu.ucr.dblab.pflock.welzl.Welzl
import org.locationtech.jts.geom._
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

object CMBC2 {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params: BFEParams = new BFEParams(args)

    implicit val S: Settings = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "CMBC",
      capacity = params.capacity(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug()
    )
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(S.scale))

    val points = readPoints(params.dataset())
    val (maximals, stats) = PSI.run(points)
    stats.printPSI()

    val vertices = points.map{_.point}
    val edges = getEdges(points)
    log(s"Reading data|START")
    case class Data(id: Int, radius: Double)
    case class Clique(points: List[Point], id: Int)

    val cliques = bk(vertices, edges).iterator.filter(_.size >= S.mu).toList.zipWithIndex.map{ case(clique, id) => Clique(clique, id)}

    @tailrec
    def itCandidates(candidates: List[Disk], final_candidates: ListBuffer[Disk]): ListBuffer[Disk] = {
      candidates match {
        case candidate :: remain_candidates => {
          val final_candidates_prime = insertDisk(final_candidates, candidate)
          itCandidates(remain_candidates, final_candidates_prime)
        }
        case Nil => final_candidates
      }
    }

    @tailrec
    def getCentres(cliques: List[Clique], r: List[Point]): List[Point] = {
      cliques match {
        case Nil => r
        case clique :: tail =>
          val mbc = Welzl.mbc(clique.points)
          val pivot = G.createPoint(new Coordinate(mbc.getCenter.getX, mbc.getCenter.getY))
          pivot.setUserData( Data(clique.id, mbc.getRadius) )
          val centres = for {
            p1 <- clique.points
            p2 <- clique.points
            if{
              val id1 = p1.getUserData.asInstanceOf[Utils.Data].id
              val id2 = p2.getUserData.asInstanceOf[Utils.Data].id
              id1 < id2
            }
          } yield {
            val centre = computeCentres(STPoint(p1), STPoint(p2)).map{ c => // pick centre closest to pivot...
              val dist = c.distance(pivot)
              (dist, c)
            }.minBy(_._1)._2
            centre
          }

          getCentres(tail, r ++ centres)
      }
    }

    @tailrec
    def getDisks(cliques: List[Clique], r: List[Disk]): List[Disk] = {
      cliques match {
        case Nil => r
        case clique :: tail =>
          val mbc = Welzl.mbc(clique.points)
          val pivot = G.createPoint(new Coordinate(mbc.getCenter.getX, mbc.getCenter.getY))
          pivot.setUserData( Data(clique.id, mbc.getRadius) )
          val centres_prime = for {
            p1 <- clique.points
            p2 <- clique.points
            if{
              val id1 = p1.getUserData.asInstanceOf[Utils.Data].id
              val id2 = p2.getUserData.asInstanceOf[Utils.Data].id
              id1 < id2
            }
          } yield {
            val centre = computeCentres(STPoint(p1), STPoint(p2)).map{ c => // pick centre closest to pivot...
              val dist = c.distance(pivot)
              (dist, c)
            }.minBy(_._1)._2

            val pids = clique.points.filter{ point => point.distance(centre) <= S.r }.map(_.getUserData.asInstanceOf[Utils.Data].id)
            Disk(centre, pids)
          }

          val centres = centres_prime.filter(_.pids.length >= S.mu)

          val r_prime = itCandidates(r, centres.to[ListBuffer])
          getDisks(tail, r_prime.toList)
      }
    }

    val centres = getCentres(cliques, List.empty[Point])
    println(centres.length)

    //val disks  = itCandidates( getDisks(cliques, List.empty[Disk]), new ListBuffer[Disk]() ).toList
    //Checker.checkMaximalDisks(disks, maximals, "CMBC", "PSI", points)

    debug{
      save("/tmp/edgesCliques.wkt") {
        cliques.map { clique =>
          val wkt = G.createMultiPoint(clique.points.toArray).toText
          val id = clique.id
          s"$wkt\t$id\n"
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
}
