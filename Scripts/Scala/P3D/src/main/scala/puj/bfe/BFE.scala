package puj.bfe

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import archery._

import puj.Params
import puj.bfe.MF_Utils._
import puj.Utils._

object BFE extends Logging {

  def run(points_prime: List[STPoint], maximals_found: RTree[Disk] = RTree.empty)
    (implicit S: Settings, G: GeometryFactory): (List[Disk], Stats) = {

    val stats = Stats()
    var Maximals: RTree[Disk] = maximals_found

    if(points_prime.isEmpty){
      (List.empty, Stats())
    } else {
      val (points, tCounts) = timer{
        computeCounts(points_prime)
      }
      stats.nPoints = points.size
      stats.tCounts = tCounts

      val (grid, tGrid) = timer{
        val G = Grid(points)
        G.buildGrid
        G
      }
      stats.tGrid = tGrid
      
      debug{
        log(s"GridSize=${grid.index.size}")
        save("/tmp/edgesPointsBFE.wkt"){ grid.pointsToText }
        save("/tmp/edgesGridBFE.wkt"){ grid.wkt() }
      }

      // for debugging purposes...
      var pairs: ListBuffer[(STPoint, STPoint)] = ListBuffer()
      val the_key = -1
      
      // for each non-empty cell...
      grid.index.keys.foreach{ key =>
        val ( (_Pr, _Ps), tRead ) = timer{
          val (i, j) = decode(key) // position (i, j) for current cell...
          val Pr = grid.index(key) // getting points in current cell...

          val indices = List( // computing positions (i, j) around current cell...
            (i-1, j+1),(i, j+1),(i+1, j+1),
            (i-1, j)  ,(i, j)  ,(i+1, j),
            (i-1, j-1),(i, j-1),(i+1, j-1)
          ).filter(_._1 >= 0).filter(_._2 >= 0) // just keep positive (i, j)...

          val Ps = indices.flatMap{ case(i, j) => // getting points around current cell...
            val key = encode(i, j)
            if(grid.index.keySet.contains(key))
              grid.index(key)
            else
              List.empty[STPoint]
          }
          (Pr, Ps)
        }
        stats.tRead += tRead
        val Pr = _Pr
        val Ps = _Ps

        debug{
          if(key == the_key) println(s"Key: ${key} Ps.size=${Ps.size}")
        }

        // for debugging purposes...
        var tCenters = 0.0
        var tCandidates = 0.0
        var tMaximals = 0.0

        val (_, tPairs) = timer{
          if(Ps.size >= S.mu){
            for{ pr <- Pr }{
              val H = pr.getNeighborhood(Ps) // get range around pr in Ps...

              debug{
                if(key == the_key) println(s"Key=${key}\t${pr.oid}\tH.size=${H.size}")
              }

              if(H.size >= S.mu){ // if range as enough points...

                for{
                  ps <- H if{ pr.oid < ps.oid }
                } yield {
                  // a valid pair...
                  stats.nPairs += 1
                  debug{
                    pairs.append{ (pr, ps) }
                  }

                  val (disks, tC) = timer{
                    // finding centers for each pair...
                    val centers = calculateCenterCoordinates(pr.point, ps.point)
                    // querying points around each center...
                    centers.map{ center =>
                      getPointsAroundCenter(center, Ps)
                    }
                  }
                  stats.nCenters += 2
                  tCenters += tC

                  val (candidates, tD) = timer{
                    // getting candidate disks...
                    disks.filter(_.count >= S.mu)
                  }
                  stats.nCandidates += candidates.size
                  tCandidates += tD

                  val (_, tM) = timer{
                    // cheking if a candidate is not a subset and adding to maximals...
                    candidates.foreach{ candidate =>
                      Maximals = insertMaximal(Maximals, candidate)
                    }
                  }
                  tMaximals += tM
                }
              }
            }
          }
        }
        stats.tCenters += tCenters
        stats.tCandidates += tCandidates
        stats.tMaximals += tMaximals
        stats.tPairs += tPairs - (tCenters + tCandidates + tMaximals)
      }

      debug{
        save("/tmp/edgesPairsBFE.wkt"){
          pairs.toList.map{ case(p1, p2) =>
            G.createLineString(Array(p1.getCoord, p2.getCoord)).toText + "\n"
          }
        }
      }

      val M = Maximals.entries.map{_.value}.toList
      stats.nMaximals = M.size

      (M, stats)
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new Params(args)

    implicit var S= Settings(
      dataset = params.input(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "BFE",
      scapacity = params.scapacity(),
      tolerance = params.tolerance(),
      debug = params.debug()
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(S.scale))

    val points = readPoints(S.dataset)
    log(s"START")

    val (maximals, stats) = BFE.run(points)
    stats.printBFE()

    debug{
      save("/tmp/edgesMaximalsBFE.wkt"){ maximals.map(_.wkt + "\n") }
      save("/tmp/edgesMaximalsBFE_prime.wkt"){ maximals.map(_.getCircleWTK + "\n") }
    }

    log(s"END")
  }
}
