package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.CMBC.Clique
import edu.ucr.dblab.pflock.PSI.insertDisk
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.pbk.PBK.bk
import org.locationtech.jts.geom._
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

object BFEStages {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params: BFEParams = new BFEParams(args)

    implicit val S: Settings = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "Test",
      capacity = params.capacity(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug()
    )
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(S.scale))

    val points = readPoints(params.dataset())
    val (maximals, stats) = PSI.run(points)
    stats.printPSI()

    def getPairs(points: List[Point]): List[LineString] = {
      val pairs = for {
        p1 <- points
        p2 <- points
        if{
          val id1 = p1.getUserData.asInstanceOf[Utils.Data].id
          val id2 = p2.getUserData.asInstanceOf[Utils.Data].id
          id1 < id2 && p1.distance(p2) <= S.epsilon
        }
      } yield {
        val coords = Array(p1.getCoordinate, p2.getCoordinate)
        G.createLineString(coords)
      }
      pairs.distinct
    }

    def getCentres(points: List[Point]): List[Point] = {
      val centres = for {
        p1 <- points
        p2 <- points
        if{
          val id1 = p1.getUserData.asInstanceOf[Utils.Data].id
          val id2 = p2.getUserData.asInstanceOf[Utils.Data].id
          id1 < id2 & p1.distance(p2) <= S.epsilon
        }
      } yield {
        val centre = computeCentres(STPoint(p1), STPoint(p2))
        centre
      }
      centres.flatten.distinct
    }

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

    def getDisks(points: List[Point]): List[Disk] = {
      val disks_prime = for {
        p1 <- points
        p2 <- points
        if{
          val id1 = p1.getUserData.asInstanceOf[Utils.Data].id
          val id2 = p2.getUserData.asInstanceOf[Utils.Data].id
          id1 < id2 & p1.distance(p2) <= S.epsilon
        }
      } yield {
        val centres = computeCentres(STPoint(p1), STPoint(p2))

        centres.map { centre =>
          val pids = points.filter { point => point.distance(centre) <= S.r }.map(_.getUserData.asInstanceOf[Utils.Data].id)
          Disk(centre, pids)
        }
      }

      val disks = disks_prime.flatten.filter(_.pids.length >= S.mu)
      itCandidates(disks, List.empty[Disk].to[ListBuffer]).toList
    }

    val P = points.map{_.point}
    var t0 = clocktime
    val pairs = getPairs(P)
    var t  = (clocktime - t0) / 1e9
    println(s"Pairs:\t$t")

    t0 = clocktime
    val centres  = getCentres(P)
    t  = (clocktime - t0) / 1e9
    println(s"Centers:\t$t")

    t0 = clocktime
    val disks  = getDisks(P)
    t  = (clocktime - t0) / 1e9
    println(s"Disks:\t$t")

    Checker.checkMaximalDisks(disks, maximals, "Test", "PSI", points)

    val factor = 1.0
    debug{
      save("/tmp/edgesPP.wkt") {
        points.map{ point =>
          val p = point.point
          val wkt = G.createPoint(new Coordinate(p.getX/factor, p.getY/factor)).toText
          s"$wkt\n"
        }
      }
      save("/tmp/edgesPL.wkt") {
        pairs.map{ pair =>
          val coords = pair.getCoordinates.map{ coord => new Coordinate(coord.x/factor, coord.y/factor)}
          val wkt = G.createLineString(coords).toText
          s"$wkt\n"
        }
      }
      save("/tmp/edgesCC.wkt") {
        centres.map { centre =>
          val wkt = G.createPoint(new Coordinate(centre.getX/factor, centre.getY/factor)).toText
          s"$wkt\t${S.r/factor}\n"
        }.distinct
      }
      save("/tmp/edgesMC.wkt") {
        maximals.map { maximal =>
          val p = maximal.center
          val wkt = G.createPoint(new Coordinate(p.getX/factor, p.getY/factor)).toText
          s"$wkt\t${S.r/factor}\n"
        }
      }
    }

    log(s"Done.|END")
  }
}
