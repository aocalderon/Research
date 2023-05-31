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
    //generateData(1000, 100, 100, "points.tsv")
    implicit val params = new BFEParams(args)
    implicit val d: Boolean = params.debug()

    implicit val settings = Settings(
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      capacity = params.capacity(),
      appId = System.nanoTime().toString(),
      tolerance = params.tolerance(),
      tag = params.tag()
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    val points = readPoints(params.input())
    logger.info("INFO|Reading data|END")

    val grid = timer(s"${settings.info}|Grid"){
      val G = Grid(points)
      G.buildGrid
      G
    }

    debug{
      save("/tmp/edgesPoints.wkt"){ grid.pointsToText }
      save("/tmp/edgesGrid.wkt"){ grid.wkt }
    }

    var Pairs = new scala.collection.mutable.ListBuffer[String]
    var nPairs = 0
    var nCenters = 0
    var Candidates = new scala.collection.mutable.ListBuffer[Disk]
    var nCandidates = 0
    val C = new STRtree(200)

    val (maximals, nMaximals) = timer(s"${settings.info}|Bfe"){
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

                debug{
                  val C = candidates.map{ candidate =>
                    val wkt  = candidate.center.toText
                    val pids = candidate.pidsText

                    s"$wkt\t$pids\n"
                  }
                  Candidates.appendAll(candidates)
                }

              }
            }
          }
        }        
      }
      // getting maximals disks...
      val maximals = pruneDisks(Candidates.toList)
      val nMaximals = maximals.size
      (maximals, nMaximals)
    }

    debug{
      save("/tmp/edgesPairs.wkt"){ Pairs.toList }
      //save("/tmp/edgesCandidates.wkt"){ Candidates.toList }
      save("/tmp/edgesMaximals.wkt"){ maximals.map(_.wkt + "\n") }
    }

    logger.info(s"total Pairs:\t${nPairs}")
    logger.info(s"totalCenters:\t${nCenters}")
    logger.info(s"totalCandidates:\t${nCandidates}")
    logger.info(s"totalMaximals:\t${nMaximals}")
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


/*
 // if c is subset of any disk in cs...
 if( cs.exists{ other => c.intersect(other).size == c.pidsSet.size } ){
 //do nothing
 } // if c is superset of one or more disks in cs...
 else if(cs.exists{ other => other.intersect(c).size == other.pidsSet.size }){
 // remove disks in cs and add c...
 cs.filter(other => other.intersect(c).size == other.pidsSet.size ).foreach{ d =>
 C.remove(d.getEnvelope(settings.r), d)
 }
 C.insert(envelope, c)
 } // neither subset or superset...
 else{
 // add c
 C.insert(envelope, c)
 }
 */

        // continue just if there are enough points...
        /* if(Ps.size >= settings.mu){
          // Range query of pr and surrounding ps...
          Pr.foreach{ pr =>
            val H = Ps.filter{ ps => pr.oid < ps.oid & pr.distance(ps) <= settings.epsilon }
            // just if H has enough points...
            if(H.size >= settings.mu){
              nPairs += H.size
              //println(s"$key\t($i, $j)\t${Pr.size}\t${Ps.size}\t${H.size}")
              val candidates_prime = H.flatMap{ pj =>
                // compute two centers from pr and pj...
                val centers = calculateCenterCoordinates(pr.point, pj.point, settings.r2)
                centers.map{ ck =>
                  // get points around each center ck...
                  val ids = H.filter{ h => ck.distance(h.point) <= settings.r }.map{_.oid.toInt}
                  val c = Disk(ck, ids)
                  c
                }.filter(_.pids.size >= settings.mu) // just disks with enough points...
              }

              // prune subset in candidates...
              //candidates = candidates ++ candidates_prime
            } 
          }
        } */ 

