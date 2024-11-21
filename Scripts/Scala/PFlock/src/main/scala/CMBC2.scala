package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.PSI.insertDisk
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.pbk.PBK.bk
import edu.ucr.dblab.pflock.welzl.Welzl
import org.locationtech.jts.geom._
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import edu.ucr.dblab.pflock.CMBC.{Clique, Data => CMBC_Data, getConvexHulls, getMBCs}

object CMBC2 {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  @tailrec
  def itCandidates(candidates: List[Disk], final_candidates: ListBuffer[Disk])(implicit G: GeometryFactory, S: Settings): ListBuffer[Disk] = {
    candidates match {
      case candidate :: remain_candidates => {
        val final_candidates_prime = insertDisk(final_candidates, candidate)
        itCandidates(remain_candidates, final_candidates_prime)
      }
      case Nil => final_candidates
    }
  }

  @tailrec
  def getCentres(cliques: List[Clique], r: List[Point])(implicit G: GeometryFactory, S: Settings): List[Point] = {
    cliques match {
      case Nil => r
      case clique :: tail =>
        val mbc = Welzl.mbc(clique.points)
        val pivot = G.createPoint(new Coordinate(mbc.getCenter.getX, mbc.getCenter.getY))
        pivot.setUserData( CMBC_Data(clique.id, mbc.getRadius) )
        val centres = for {
          p1 <- clique.points
          p2 <- clique.points
          if{
            val id1 = p1.getUserData.asInstanceOf[Utils.Data].id
            val id2 = p2.getUserData.asInstanceOf[Utils.Data].id
            id1 < id2
          }
        } yield {
          val centre = computeCentres(STPoint(p1), STPoint(p2))
          centre
        }
        val r_prime = (r ++ centres.flatten).distinct
        getCentres(tail, r_prime)
    }
  }

  @tailrec
  def getDisks(cliques: List[Clique], r: List[Disk])(implicit G: GeometryFactory, S: Settings): List[Disk] = {
    cliques match {
      case Nil => r
      case clique :: tail =>
        val mbc = Welzl.mbc(clique.points)
        val pivot = G.createPoint(new Coordinate(mbc.getCenter.getX, mbc.getCenter.getY))
        pivot.setUserData( CMBC_Data(clique.id, mbc.getRadius) )
        val disks_prime = for {
          p1 <- clique.points
          p2 <- clique.points
          if{
            val id1 = p1.getUserData.asInstanceOf[Utils.Data].id
            val id2 = p2.getUserData.asInstanceOf[Utils.Data].id
            id1 < id2
          }
        } yield {
          val centres = computeCentres(STPoint(p1), STPoint(p2))

          centres.map { centre =>
            val pids = clique.points.filter { point => point.distance(centre) <= S.r }.map(_.getUserData.asInstanceOf[Utils.Data].id)
            Disk(centre, pids)
          }
        }

        val disks = disks_prime.flatten.filter(_.pids.length >= S.mu)

        val r_prime = itCandidates(r, disks.to[ListBuffer])
        getDisks(tail, r_prime.toList)
    }
  }
  def main(args: Array[String]): Unit = {
    implicit val params: BFEParams = new BFEParams(args)

    implicit var S: Settings = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "",
      capacity = params.capacity(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug()
    )
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(S.scale))

    val points = readPoints(params.dataset())
    S = S.copy(method = "PSI")
    val (maximals_psi, stats_prime) = PSI.run(points)
    stats_prime.printPSI()

    var total = 0.0
    S = S.copy(method = "CMBC")
    var t0 = clocktime
    val vertices = points.map{_.point}
    val edges = getEdgesByDistance(points, S.epsilon)
    var t  = (clocktime - t0) / 1e9
    logt(s"Graph    |$t")
    total = total + t

    t0 = clocktime
    val cliques = bk(vertices, edges).iterator.filter(_.size >= S.mu).toList.zipWithIndex.map{ case(clique, id) => Clique(clique, id)}
    t  = (clocktime - t0) / 1e9
    logt(s"Cliques  |$t")
    total = total + t

    t0 = clocktime
    val (enclosed, not_enclosed) = cliques.map{ clique =>
      val mbc = Welzl.mbc(clique.points)
      (clique.points, mbc)
    }.partition(_._2.getRadius <= S.r)
    val maximals_enclosed = enclosed.map{ case(points, mbc) =>
      val pivot = G.createPoint(new Coordinate(mbc.getCenter.getX, mbc.getCenter.getY))
      val pids = points.map{ point => point.getUserData.asInstanceOf[Data].id }
      Disk(pivot, pids)
    }
    val points_prime = not_enclosed.flatMap{ case(points, _) =>
      points.map(STPoint(_))
    }.groupBy{_.oid}.map{ case(_, points) => points.head }.toList
    t  = (clocktime - t0) / 1e9
    logt(s"MBC      |$t")
    total = total + t

    t0 = clocktime
    val (maximals_not_enclosed, stats) = PSI.run(points_prime)
    t  = (clocktime - t0) / 1e9
    logt(s"Centers    |$t")
    total = total + t

    t0 = clocktime
    val maximals_cmbc = itCandidates( maximals_enclosed ++ maximals_not_enclosed, new ListBuffer[Disk]() ).toList
    t  = (clocktime - t0) / 1e9
    logt(s"Prune      |$t")
    total = total + t

    logt(s"Total      |$total")

    debug{
      Checker.checkMaximalDisks(maximals_cmbc, maximals_psi, "CMBC", "PSI", points)
      
      save("/tmp/edgesP.wkt") {
        points.map{ point =>
          val wkt = point.point.toText
          s"$wkt\n"
        }
      }
      save("/tmp/edgesMPSI.wkt") {
        maximals_psi.map { maximal =>
          val wkt  = maximal.getCircleWTK
          s"$wkt\t${S.r}\n"
        }
      }
      save("/tmp/edgesMCMBC.wkt") {
        maximals_cmbc.map { maximal =>
          val wkt  = maximal.getCircleWTK
          s"$wkt\t${S.r}\n"
        }
      }
      save("/tmp/edgesME.wkt") {
        maximals_enclosed.map { maximal =>
          val wkt  = maximal.getCircleWTK
          s"$wkt\t${S.r}\n"
        }
      }
      save("/tmp/edgesMNE.wkt") {
        maximals_not_enclosed.map { maximal =>
          val wkt  = maximal.getCircleWTK
          s"$wkt\t${S.r}\n"
        }
      }
      save("/tmp/edgesMBC.wkt") {
        val mbcs  = getMBCs(cliques, List.empty[Point])
        mbcs.map { mbc =>
          val radius = round3(mbc.getUserData.asInstanceOf[CMBC_Data].radius)
          val wkt = mbc.buffer(radius, 25).toText
          s"$wkt\t$radius\n"
        }
      }
      save("/tmp/edgesCH.wkt") {
        val chs   = getConvexHulls(cliques, List.empty[Polygon])
        chs.map { ch =>
          val wkt = ch.toText
          val n   = ch.getCoordinates.length
          s"$wkt\t$n\n"
        }
      }
    }

    log(s"Done.|END")
  }
}
