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

import com.github.davidmoten.rtree2._
import com.github.davidmoten.rtree2.geometry._

object BFE_david {
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
    
    val points = readPoints(params.input())
    logger.info("INFO|Reading data|END")

    val grid = timer(s"${settings.appId}|Grid"){
      buildGrid(points, settings.epsilon_prime)
    }

    var T_david: RTree[Disk, geometry.Point] = RTree.create()

    timer(s"${settings.info}|David"){
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
                      
                      T_david = T_david.add(c, Geometries.point(c.X, c.Y));

                    }
                  }
                }
              }
            }
          }
        }
      }
    logger.info(s"INFO|Candidate disks|${T_david.size()}")
  }
}
