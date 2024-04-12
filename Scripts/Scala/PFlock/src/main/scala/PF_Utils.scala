package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils.{Disk, STPoint, Settings, debug}
import org.locationtech.jts.geom.GeometryFactory
import org.slf4j.Logger

import scala.annotation.tailrec

object PF_Utils {
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
  def join(trajs: List[(Int, Iterable[STPoint])], flocks: List[Disk], n: Int)(implicit S: Settings, G: GeometryFactory, L: Logger): Int = {

    trajs match {
      case current_trajs :: remaining_trajs =>
        val time = current_trajs._1
        val points = current_trajs._2.toList

        val (new_flocks, stats) = if(S.method == "BFE")
          BFE.run(points)
        else {
          val (nf, stats) = PSI.run(points)
          (nf.map(_.copy(start = time, end = time)), stats)

          //PSI.run(points)
        }

        debug{
          stats.printPSI()
        }

        /***
         * start: merging previous flocks with current flocks...
         ***/
        val old_flocks = if(S.method != "BFE") {
          //println(s"PSI")
          val inverted_index = flocks.flatMap { flock =>
            flock.pids.map { pid =>
              pid -> flock
            }
          }.groupBy(_._1).mapValues {
            _.map(_._2)
          }

          val flocks_prime = for {new_flock <- new_flocks} yield {
            val disks = new_flock.pids.filter{ pid => inverted_index.keySet.contains(pid) }.flatMap { pid =>
              inverted_index(pid)
            }.distinct
            disks
          }
          flocks_prime.flatten

          //flocks
        } else {
          //println(s"BFE")
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

}
