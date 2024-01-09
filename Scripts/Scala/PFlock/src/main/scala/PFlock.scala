package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point, LineString}
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.io.WKTReader

import org.slf4j.{Logger, LoggerFactory}

import scala.xml._
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import java.io.FileWriter

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SaveMode}

import edu.ucr.dblab.pflock.MF_Utils._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.sedona.quadtree.Quadtree

object PFlock {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("PFlock").getOrCreate()
    import spark.implicits._

    implicit val S = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      delta = params.delta(),
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      print = params.print(),
      output = params.output(),
      iindex = params.iindex(),
      appId = spark.sparkContext.applicationId
    )

    implicit val G = new GeometryFactory(new PrecisionModel(S.scale))

    printParams(args)
    log(s"START|")

    /*******************************************************************************/
    // Code here...

    val trajs = spark.read
      .option("header", false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .mapPartitions{ rows =>
        rows.map{ row =>
          val oid = row.getString(0).toInt
          val lon = row.getString(1).toDouble
          val lat = row.getString(2).toDouble
          val tid = row.getString(3).toInt

          val point = G.createPoint(new Coordinate(lon, lat))
          point.setUserData(Data(oid, tid))

          (tid, STPoint(point))
        }
      }.groupByKey().sortByKey().collect.toList

    debug{
      trajs.foreach(println)
    }

    @tailrec
    def pruneM(M: List[Disk], M_prime: List[Disk]): List[Disk] = {
      M match {
        case new_flock::tail =>
          var stop = false
          for{old_flock <- tail if !stop}{
            val count = old_flock.pidsSet.intersect(new_flock.pidsSet).size

            count match {
              case count if new_flock.pids.size == count =>
                if(old_flock.start <= new_flock.start){
                  stop = true // new is contained by a previous old...
                } else if(old_flock.pids.size > count){
                  // old and new do not have the same points.  We iterate next...
                } else {
                  old_flock.subset = true // old is a subset of the new one.  We need to remove it...
                }
              case count if old_flock.pids.size == count =>
                if(old_flock.start < new_flock.start){
                  // old is not a subset of new one...
                } else {
                  old_flock.subset = true // old is a subset of the new one.  We need to remove it...
                }
              case _ =>
                // old and new have different points.  We iterate next...
            }
          }

          if(!stop)
            pruneM(tail, M_prime :+ new_flock)
          else
            pruneM(tail, M_prime)

        case Nil => M_prime
      }
    }

    @tailrec
    def pruneN(M: List[Disk], N: List[Disk], N_prime: List[Disk]): List[Disk] = {
      N match {
        case n::tail =>
          if( M.exists(m => n.pids.size == n.pidsSet.intersect(m.pidsSet).size) ){
            pruneN(M, tail, N_prime)
          } else {
            pruneN(M, tail, N_prime :+ n)
          }
        case Nil => N_prime
      }
    }

    @tailrec
    def join(trajs: List[(Int, Iterable[STPoint])], flocks: List[Disk], n: Int)(implicit S: Settings): Int = {

      trajs match {
        case current_trajs :: remaining_trajs =>
          val time = current_trajs._1
          val points = current_trajs._2.toList

          val (new_flocks, stats) = if(S.method == "BFE")
            BFE.run(points)
          else
            PSI.run(points)

          debug{
            stats.printPSI()
          }

          /***
           * start: merging previous flocks with current flocks...
           ***/
          val old_flocks = if(S.iindex && flocks.nonEmpty) {
            val inverted_index = flocks.flatMap { flock =>
              flock.pids.map { pid =>
                pid -> flock
              }
            }.groupBy(_._1).mapValues {
              _.map(_._2)
            }
            val flocks_prime = for {new_flock <- new_flocks} yield {
              val disks = new_flock.pids.filter{ pid => inverted_index.keySet.contains(pid) }.map { pid =>
                inverted_index(pid).toSet
              }
              disks.reduce(_ intersect _).toList
            }
            flocks_prime.flatten
          } else {
            flocks
          }

          val merged_ones = (for{
            old_flock <- old_flocks
            new_flock <- new_flocks
          } yield {
            val pids = old_flock.pidsSet.intersect(new_flock.pidsSet).toList
            val flock = Disk(new_flock.center, pids, old_flock.start, time)

            if(pids == new_flock.pids) new_flock.subset = true

            flock
          }).filter(_.pids.size >= S.mu) // filtering by minimum number of entities (mu)...
          /***
           * end: merging previous flocks with current flocks...
           ***/

          /***
           * start: pruning subset flocks...
           ***/
          val M = pruneM(merged_ones, List.empty[Disk]).filterNot(_.subset)

          val N = new_flocks.filterNot(_.subset).map{ flock =>
            Disk(flock.center, flock.pids, time, time)
          }

          val candidates = M ++ pruneN(M, N, List.empty[Disk])
          /***
           * end: pruning subset flocks...
           ***/

          /***
           * start: reporting...
           ***/
          val F_prime = candidates
            .map{ flock =>
              val a = flock.end - flock.start
              val b = S.delta - 1

              (flock, a >= b)
            }
          val count = n + F_prime.count(_._2)
          if(S.print){
            F_prime.filter(_._2).map(_._1).foreach{println}
          }
          /***
           * end: reporting...
           ***/

          /***
           * start: recurse...
           ***/
          val F = F_prime
            .map{ case(flock, mustUpdate) =>
              if(mustUpdate) flock.copy(start = flock.start + 1) else flock
            }

          join(remaining_trajs, F, count)
          /***
           * start: recurse...
           ***/
        case Nil => n
      }
    }

    val flocks = join(trajs, List.empty[Disk], 0)

    log("Done!")
    log(s"Number of flocks:\t${flocks}")

    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }
}
