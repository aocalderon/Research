package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.MF_Utils._
import archery._


object BFE {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def run(points_prime: List[STPoint], maximals_found: RTree[Disk] = RTree.empty)
    (implicit settings: Settings, geofactory: GeometryFactory):
      (List[Disk], Stats) = {

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
        save("/tmp/edgesPoints.wkt"){ grid.pointsToText }
        save("/tmp/edgesGrid.wkt"){ grid.wkt() }
      }

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

        var tCenters = 0.0
        var tCandidates = 0.0
        var tMaximals = 0.0

        val (_, tPairs) = timer{
          if(Ps.size >= settings.mu){
            for{ pr <- Pr }{
              val H = pr.getNeighborhood(Ps) // get range around pr in Ps...

              debug{
                if(key == the_key) println(s"Key=${key}\t${pr.oid}\tH.size=${H.size}")
              }

              if(H.size >= settings.mu){ // if range as enough points...

                for{
                  ps <- H if{ pr.oid < ps.oid }
                } yield {
                  // a valid pair...
                  stats.nPairs += 1

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
                    disks.filter(_.count >= settings.mu)
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

      val M = Maximals.entries.map{_.value}.toList
      stats.nMaximals = M.size

      (M, stats)
    }
  }

  def runLCM(points: List[STPoint])
    (implicit settings: Settings, geofactory: GeometryFactory, debugOn: Boolean):
      (List[Disk], Stats) = {

    val stats = Stats()
    var Pairs = new scala.collection.mutable.ListBuffer[String]
    var C = new scala.collection.mutable.ListBuffer[Disk]

    if(points.isEmpty){
      (C.toList, Stats())
    } else {
      val (grid, tGrid) = timer{
        val G = Grid(points)
        G.buildGrid
        G
      }

      debug{
        log(s"${grid.index.size}")
        save("/tmp/edgesPoints.wkt"){ grid.pointsToText }
        save("/tmp/edgesGrid.wkt"){ grid.wkt() }
      }

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

        var tCenters = 0.0
        var tCandidates = 0.0
        var tMaximals = 0.0

        val (_, tPairs) = timer{
          if(Ps.size >= settings.mu){
            for{ pr <- Pr }{
              val H = pr.getNeighborhood(Ps) // get range around pr in Ps...

              debug{
                if(key == the_key) println(s"Key=${key}\t${pr.oid}\tH.size=${H.size}")
              }

              if(H.size >= settings.mu){ // if range as enough points...

                for{
                  ps <- H if{ pr.oid < ps.oid }
                } yield {
                  // a valid pair...
                  stats.nPairs += 1

                  debug{
                    val p1  = pr.oid
                    val p2  = ps.oid
                    val lin = geofactory.createLineString(Array(pr.getCoord, ps.getCoord))
                    val wkt = lin.toText
                    val len = lin.getLength

                    val P = s"$wkt\t$p1\t$p2\t$len\t$key\n"
                    Pairs.append(P)
                  }

                  val (_, tC) = timer{
                    // finding centers for each pair...
                    val centers = calculateCenterCoordinates(pr.point, ps.point)
                    // querying points around each center...
                    val disks = centers.map{ center =>
                      getPointsAroundCenter(center, Ps)
                    }
                    C.appendAll(disks.filter(_.count >= settings.mu))
                  }
                  stats.nCenters += 2
                  tCenters += tC

                }
              }
            }
          }
        }
        stats.tGrid = tGrid
        stats.tCenters += tCenters
        stats.tPairs += tPairs - (tCenters + tCandidates + tMaximals)
      }

      debug{
        save("/tmp/edgesPairs.wkt"){ Pairs.toList }
        save("/tmp/edgesCandidates.wkt"){ C.map{_.wkt + "\n"} }
      }

      val (m, tM) = timer{ pruneDisks(C.toList) }
      stats.tMaximals = tM
      stats.nPoints = points.size
      stats.nMaximals = m.size

      (m, stats)
    }
  }

  def main(args: Array[String]): Unit = {
    //generateData(10000, 1000, 1000, "/home/acald013/Research/Datasets/P10K_W1K_H1K.tsv")
    implicit val params = new BFEParams(args)

    implicit var settings = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "BFE",
      capacity = params.capacity(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(), 
      tester = params.tester(),
      appId = System.nanoTime().toString()
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    val points = readPoints(settings.dataset)
    log(s"START")

    val (maximals, stats1) = BFE.run(points)
    stats1.print()

    debug{
      save("/tmp/edgesBFE_M.wkt"){ maximals.map(_.wkt + "\n") }
      save("/tmp/edgesBFE_M_prime.wkt"){ maximals.map(_.getCircleWTK + "\n") }
    }

    if(settings.tester){
      save("/tmp/edgesBFE_M.wkt"){ maximals.map(_.wkt + "\n") }
    }

    log(s"END")
  }
}
