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

import archery._

object BFE {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
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
    val properties = System.getProperties().asScala
    logger.info(s"COMMAND|${properties("sun.java.command")}")
    
    logger.info("INFO|Reading data|START")
    val points = readPoints(params.input())
    logger.info("INFO|Reading data|END")

    val grid = timer(s"${settings.appId}|Grid"){
      buildGrid(points, settings.epsilon_prime)
    }

    debug{
      save("/tmp/edgesP.wkt"){
        grid.map{ case(key, points) =>
          val (i, j) = decode(key)
          points.map{ point =>
            val wkt = point.toText
            val oid = point.oid

            s"$wkt\t$key\t($i $j)\t$oid\n"
          }
        }.flatten.toList
      }
    }

    var T: RTree[Disk] = RTree()
    val C = new STRtree(200) 
    grid.filter(_._2.size >= 0) // just over non-empty cells...
      .keys.foreach{ key =>
        val (i, j) = decode(key) // position (i, j) for current cell...
        val Pr = grid(key) // getting points in current cell...
        val indices = List( // computing positions (i, j) around current cell...
          (i-1, j+1),(i, j+1),(i+1, j+1),
          (i-1, j)  ,(i, j)  ,(i+1, j),
          (i-1, j-1),(i, j-1),(i+1, j-1)
        ).filter(_._1 >= 0).filter(_._2 >= 0) // just keep positive (i, j)...
        val Ps = indices.flatMap{ case(i, j) => // getting points around current cell...
          val key = encode(i, j)
          if(grid.keySet.contains(key))
            grid(key)
          else
            List.empty[STPoint]
        }
        if(Ps.size >= settings.mu){ // continue just if there are enough points...
          Pr.foreach{ pr =>
            // Range query of pr and surrounding ps...
            val H = Ps.filter{ ps => pr.distance(ps) <= settings.epsilon && pr.oid < ps.oid }
            // just if H has enough points...
            if(H.size >= settings.mu){
              //println(s"$key\t($i, $j)\t${Pr.size}\t${Ps.size}\t${H.size}")
              H.foreach{ pj =>
                // compute two centers from pr and pj...
                val cs = calculateCenterCoordinates(pr.point, pj.point, settings.r2)
                cs.foreach{ ck =>
                  // get points around each center ck...
                  val ids = H.filter{ h => ck.distance(h.point) <= settings.r2 }.map{_.oid.toInt}
                  if(ids.size >= settings.mu){ // just if disk have enough points...
                    val c = Disk(ck, ids)
                  
                    C.insert(c.envelope, c)
                    val cs: Seq[Entry[Disk]] = T.search(c.bbox(settings.epsilon))
                    println(cs.size)
                    T = T.insert(c.X, c.Y, c)
                  }
                }
              }
            }
          }
        }
      }
    println(s"Candidate disks: ${C.size()}")
    println(s"T size: ${T.size}")
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
