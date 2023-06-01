package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point}
import com.vividsolutions.jts.index.strtree.STRtree
import org.datasyslab.geospark.spatialRDD.SpatialRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

import edu.ucr.dblab.pflock.quadtree._
import edu.ucr.dblab.pflock.Utils._

object BFE {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    //generateData(10000, 1000, 1000, "/home/acald013/Research/Datasets/P10K_W1K_H1K.tsv")
    implicit val params = new BFEParams(args)
    implicit val d: Boolean = params.debug()

    implicit val settings = Settings(
      input = params.input(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      capacity = params.capacity(),
      appId = System.nanoTime().toString(),
      tolerance = params.tolerance(),
      tag = params.tag()
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    val points = readPoints(params.input())
    logger.info(s"INFO|${settings.info}|Reading data|START")

    val grid = timer(s"${settings.info}|Grid"){
      val G = Grid(points)
      G.buildGrid
      G
    }

    debug{
      save("/tmp/edgesPoints.wkt"){ grid.pointsToText }
      save("/tmp/edgesGrid.wkt"){ grid.wkt() }
    }

    var Pairs = new scala.collection.mutable.ListBuffer[String]
    var nPairs = 0
    var nCenters = 0
    var Candidates = new scala.collection.mutable.ListBuffer[Disk]
    var nCandidates = 0
    var Maximals = List.empty[Disk]

    timer(s"${settings.info}|Bfe"){
      val the_key = -1
      // for each non-empty cell...
      grid.index.filter(_._2.size > 0).keys.foreach{ key =>
        val (i, j) = decode(key) // position (i, j) for current cell...
        val Pr = grid.index(key) // getting points in current cell...

        val indices = List( // computing positions (i, j) around current cell...
          (i-1, j+1),(i, j+1),(i+1, j+1),
          (i-1, j)  ,(i, j)  ,(i+1, j),
          (i-1, j-1),(i, j-1),(i+1, j-1)
        ).filter(_._1 >= 0).filter(_._2 >= 0) // just keep positive (i, j)...

        debug{
          if(key == the_key) println(s"($i $j) => ${indices.sortBy(_._1).sortBy(_._2).mkString(" ")}")
        }

        val Ps = indices.flatMap{ case(i, j) => // getting points around current cell...
          val key = encode(i, j)
          if(grid.index.keySet.contains(key))
            grid.index(key)
          else
            List.empty[STPoint]
        }

        debug{
          if(key == the_key) println(s"Key: ${key} Ps.size=${Ps.size}")
        }

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
                nPairs += 1

                debug{
                  val p1  = pr.oid
                  val p2  = ps.oid
                  val lin = geofactory.createLineString(Array(pr.getCoord, ps.getCoord))
                  val wkt = lin.toText
                  val len = lin.getLength

                  val P = s"$wkt\t$p1\t$p2\t$len\t$key\n"
                  Pairs.append(P)
                }

                // finding centers for each pair...
                val centers = calculateCenterCoordinates(pr.point, ps.point)
                // querying points around each center...
                val disks = centers.map{ center =>
                  getPointsAroundCenter(center, Ps)
                }
                nCenters += 2

                // getting candidate disks...
                val candidates = disks.filter(_.count >= settings.mu)
                nCandidates += candidates.size

                Candidates.appendAll(candidates)

                candidates.foreach{ candidate =>
                  Maximals = insertMaximal(Maximals, candidate)
                }

                debug{
                  val C = candidates.map{ candidate =>
                    val wkt  = candidate.center.toText
                    val pids = candidate.pidsText

                    s"$wkt\t$pids\n"
                  }
                }
              }
            }
          }
        }        
      }
    }

    val nMaximals = Maximals.size
    debug{
      save("/tmp/edgesPairs.wkt"){ Pairs.toList }
      //save("/tmp/edgesCandidates.wkt"){ Candidates.toList }
      save("/tmp/edgesMaximals.wkt"){ Maximals.map(_.wkt + "\n") }
    }

    logger.info(s"INFO|${settings.info}|Pairs|${nPairs}")
    logger.info(s"INFO|${settings.info}|Centers|${nCenters}")
    logger.info(s"INFO|${settings.info}|Candidates|${nCandidates}")
    logger.info(s"INFO|${settings.info}|Maximals|${nMaximals}")

    debug{ checkMaximals(points) }
  }
}

import org.rogach.scallop._

class BFEParams(args: Seq[String]) extends ScallopConf(args) {
  val tolerance: ScallopOption[Double]  = opt[Double]  (default = Some(1e-3))
  val input:     ScallopOption[String]  = opt[String]  (default = Some(""))
  val epsilon:   ScallopOption[Double]  = opt[Double]  (default = Some(10.0))
  val mu:        ScallopOption[Int]     = opt[Int]     (default = Some(5))
  val capacity:  ScallopOption[Int]     = opt[Int]     (default = Some(100))
  val tag:       ScallopOption[String]  = opt[String]  (default = Some(""))
  val output:    ScallopOption[String]  = opt[String]  (default = Some("/tmp"))
  val debug:     ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
