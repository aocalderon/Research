package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point}
import org.datasyslab.geospark.spatialRDD.SpatialRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

import edu.ucr.dblab.pflock.quadtree._
import edu.ucr.dblab.pflock.Utils._

import archery._

object BFE_CMBC {

  def runAtBegining(points: List[STPoint])
    (implicit settings: Settings, geofactory: GeometryFactory, debugOn: Boolean):
      (List[Disk], Stats) = {

    // 1. find cliques...
    // 2. find MBC in each clique...
    // 3. divide MBCs by radius less than epsilon...
    // 4. return MBCs greater than epsilon as maximal disks...
    // 5. return remaining points in MBCs greater than epsilon as list of points...
    val ( (maximals_prime, points_prime, maximals_found), tCli) = timer{
      val (maximals_prime, points_prime) = partitionByRadius(getMBCsPerClique(points))
      val maximals_found = RTree[Disk]().insertAll(maximals_prime.map{ maximal =>
        Entry(archery.Point(maximal.X, maximal.Y), maximal)
      })
      (maximals_prime, points_prime, maximals_found)
    }
    // Run traditial BFE with the remaining points and maximals found...
    val (maximals, stats) = BFE.run(points_prime, maximals_found)
    val stats_update = stats.copy(nPoints = points_prime.size,
      nMaximals = maximals.size,
      tCliques = tCli)
    (maximals, stats_update)
  }

  def runByGrid(points: List[STPoint])
    (implicit settings: Settings, geofactory: GeometryFactory, debugOn: Boolean):
      (List[Disk], Stats) = {

    val stats = Stats()
    var Pairs = new scala.collection.mutable.ListBuffer[String]
    var Maximals: RTree[Disk] = RTree()

    if(points.isEmpty){
      (List.empty[Disk], Stats())
    } else {
      val (grid, tGrid) = timer{
        val G = Grid(points)
        G.buildGrid
        G
      }

      debug{
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

        val ( (pr_prime, ps_prime), tCliques) = timer{
          // filtering by Cliques and MBCs...
          val (maximals1, ps_prime) = partitionByRadius(getMBCsPerClique(Ps))
          // inserting new maximals...
          Maximals = insertMaximals(Maximals, maximals1)
          // filtering by points already in maximals...
          val pr_prime = Pr.filterNot(maximals1.map{_.pidsSet}.flatten.toSet)

          (pr_prime, ps_prime)
        }
        stats.nPoints += pr_prime.size
        stats.tCliques += tCliques

        debug{
          if(key == the_key) println(s"Key: ${key} Ps.size=${Ps.size}")
        }

        var tCenters = 0.0
        var tCandidates = 0.0
        var tMaximals = 0.0

        val (_, tPairs) = timer{
          if(ps_prime.size >= settings.mu){ // testing on new list ps_prime...

            for{ pr <- pr_prime }{ // iterating over new list pr_prime...
              val H = pr.getNeighborhood(ps_prime) // get range around pr in Ps...

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
        stats.tGrid = tGrid
        stats.tCenters += tCenters
        stats.tCandidates += tCandidates
        stats.tMaximals += tMaximals
        stats.tPairs += tPairs - (tCenters + tCandidates + tMaximals)
      }

      debug{
        save("/tmp/edgesPairs.wkt"){ Pairs.toList }
      }

      stats.nMaximals = Maximals.entries.size
      
      (Maximals.entries.map{_.value}.toList, stats)
    }
  }  

  def main(args: Array[String]): Unit = {
    //generateData(10000, 1000, 1000, "/home/acald013/Research/Datasets/P10K_W1K_H1K.tsv")
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    implicit val params = new BFEParams(args)
    implicit val d: Boolean = params.debug()

    implicit var settings = Settings(
      input = params.input(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = params.method(),
      capacity = params.capacity(),
      appId = System.nanoTime().toString(),
      tolerance = params.tolerance(),
      tag = params.tag()
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    val points = readPoints(params.input())
    log(s"Reading data|START")

    settings = settings.copy(method="BFE_CMBC1")
    val (maximals, stats1) = BFE_CMBC.runAtBegining(points)
    stats1.print()

    settings = settings.copy(method="BFE_CMBC2")
    val (_, stats2) = BFE_CMBC.runByGrid(points)
    stats2.print()

    settings = settings.copy(method="BFE")
    val (_, stats3) = BFE.run(points)
    stats3.print()

    debug{
      save("/tmp/edgesMaximals.wkt"){ maximals.map(_.wkt + "\n") }
      settings = settings.copy(method="BFE0")
      checkMaximals(points)
    }

    log(s"Done.|END")
  }
}
