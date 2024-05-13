package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils.{Disk, STPoint, Settings, debug}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Envelope, GeometryFactory, Point}
import org.slf4j.Logger

import scala.annotation.tailrec
import scala.collection.mutable

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
  def pruneQ(Q: List[Disk], Q_prime: List[Disk]): List[Disk] = {
    Q match {
      case flock1::tail =>
        var stop = false
        for{flock2 <- tail if !stop}{
          val count = flock2.pidsSet.intersect(flock1.pidsSet).size

          count match {
            case count if flock1.pids.size == count =>
              if(flock2.start <= flock1.start && flock1.end <= flock2.end){
                stop = true // flock1 is contained by flock2...
              } else if(flock2.pids.size > count){
                // flock1 and flock2 do not have the same points.  We iterate next...
              } else {
                if(flock2.start < flock1.start || flock1.end < flock2.end){
                  // flock2 is not a subset of flock1 (they have same pids but differ in time).  We iterate next..
                } else {
                  flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
                }
              }
            case count if flock2.pids.size == count =>
              if(flock2.start < flock1.start || flock1.end < flock2.end){
                // flock2 is not a subset of flock1.  We iterate next..
              } else {
                flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
              }
            case _ =>
            // flock1 and flock2 have different points.  We iterate next...
          }
        }

        if(!stop)
          pruneQ(tail, Q_prime :+ flock1)
        else
          pruneQ(tail, Q_prime)

      case Nil => Q_prime.filterNot(_.subset)
    }
  }

  def pruneP(P: List[Disk]): List[Disk] = pruneQ(P, List.empty[Disk])

  def prune2(Q: List[Disk], R: List[Disk], Q_prime: List[Disk]): List[Disk] = {
    Q match {
      case flock1::tail =>
        var stop = false
        for{flock2 <- R if !stop}{
          val count = flock2.pidsSet.intersect(flock1.pidsSet).size

          count match {
            case count if flock1.pids.size == count =>
              if(flock2.start <= flock1.start && flock1.end <= flock2.end){
                stop = true // flock1 is contained by flock2...
              } else if(flock2.pids.size > count){
                // flock1 and flock2 do not have the same points.  We iterate next...
              } else {
                if(flock2.start < flock1.start || flock1.end < flock2.end){
                  // flock2 is not a subset of flock1 (they have same pids but differ in time).  We iterate next..
                } else {
                  flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
                }
              }
            case count if flock2.pids.size == count =>
              if(flock2.start < flock1.start || flock1.end < flock2.end){
                // flock2 is not a subset of flock1.  We iterate next..
              } else {
                flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
              }
            case _ =>
            // flock1 and flock2 have different points.  We iterate next...
          }
        }

        if(!stop)
          prune2(tail, R.filterNot(_.subset), Q_prime :+ flock1)
        else
          prune2(tail, R.filterNot(_.subset), Q_prime)

      case Nil => Q_prime ++ R
    }
  }

  @tailrec
  def join(trajs: List[(Int, Iterable[STPoint])], flocks: List[Disk], f: List[Disk])(implicit S: Settings, G: GeometryFactory, L: Logger): List[Disk] = {

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
        //val count = n + F_prime.count(_._2)
        val r = F_prime.filter(_._2).map(_._1)
        if(S.print){
          r.foreach{println}
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

        join(remaining_trajs, F, f ++ r)
      /***
       * start: recurse...
       ***/
      case Nil => f
    }
  }

  def joinDisks(trajs: List[(Int, Iterable[STPoint])], flocks: List[Disk], f: List[Disk], cell: Envelope, cell_prime: Envelope, partial: List[Disk])
               (implicit S: Settings, G: GeometryFactory, L: Logger): (List[Disk], List[Disk]) = {
    val pid = TaskContext.getPartitionId()
    //if(pid == 53)
      //println{s"Cell: $pid"}
    trajs match {
      case current_trajs :: remaining_trajs =>
        val time = current_trajs._1
        val points = current_trajs._2.toList

        val (new_flocks, stats) = if (S.method == "BFE") {
          val (nf, stats) = BFE.run(points)
          (nf.map(_.copy(start = time, end = time)).filter(d => cell_prime.contains(d.center.getCoordinate)), stats)
          //(nf.map(_.copy(start = time, end = time)), stats)
        } else {
          val (nf, stats) = PSI.run(points)
          (nf.map(_.copy(start = time, end = time)).filter(d => cell_prime.contains(d.center.getCoordinate)), stats)
          //(nf.map(_.copy(start = time, end = time)), stats)
        }

        debug{
          //stats.printPSI()
          new_flocks.map{ f =>
            val wkt  = f.center.toText
            val pids = f.pidsText
            s"$wkt\t$pids\t$pid\t$time"
          }.filter(_ => 2 <= time && time <= 5).foreach{println}
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
          flock.locations = old_flock.locations :+ new_flock.center.getCoordinate

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
            val safe_delta = flock.end - flock.start >= S.delta - 1
            val safe_area  = cell.contains(flock.locations.head) || cell.contains(flock.locations.last) // report if flock in in safe area

            (flock, safe_delta, safe_area)
          }

        val r = F_prime.filter(f => f._2 && f._3).map(_._1)
        if(S.print){
          r.foreach{println}
        }
        /***
         * end: reporting...
         ***/

        /***
         * start: recurse...
         ***/

        val F = F_prime
          .map{ case(flock, safe_delta, _) =>
            if(safe_delta) {
              val f = flock.copy(start = flock.start + 1)
              f.locations = flock.locations.tail
              f
            } else flock
          }

        //val F_partial = getPartials(F, cell)
        val F_partial = F_prime.filterNot(_._3).map(_._1)

        joinDisks(remaining_trajs, F, f ++ r, cell, cell_prime, partial ++ F_partial)
      /***
       * start: recurse...
       ***/
      case Nil => (f, partial)
    }
  }

  def funPartial(f: List[Disk], time: Int, partials: mutable.HashMap[Int, List[Disk]], result: List[Disk])
                (implicit S: Settings): (List[Disk], List[Disk]) = {
    val seeds = try {
      partials(time)
    } catch {
      case e: Exception => List.empty[Disk]
    }
    val flocks = f ++ pruneP( seeds )

    val D = for(flock1 <- flocks) yield {

      val partial_prime = try {
        partials(flock1.end + 1)
      } catch {
        case e: Exception => List.empty[Disk]
      }

      partial_prime.filter{ flock2 =>
        flock1.pidsSet.intersect(flock2.pidsSet).size >= S.mu //&& flock1.did != flock2.did
      }.map { flock2 =>
        val pids  = flock1.pidsSet.intersect(flock2.pidsSet).toList
        val start = flock1.start
        val end   = flock2.end

        val d = Disk(flock2.center, pids, start, end)
        d.locations = flock1.locations ++ flock2.locations
        d.did = flock2.did
        d
      }
    }
    val candidates = pruneP(flocks ++ D.flatten)

    val F_prime = candidates.map{ f =>
      val a = f.start == time - (S.delta - 1)
      (f, a)
    }
    val r = F_prime.filter(_._2).map{ case(f, _) =>
      f.copy(end = time)
    }
    val result_prime = pruneP(r)
    //result_prime.foreach{println}

    val F = F_prime
      .map{ case(flock, mustUpdate) =>
        if(mustUpdate) {
          val f = flock.copy(start = flock.start + 1)
          f.locations = flock.locations.tail
          f.did = flock.did
          f
        } else flock
      }.filter( _.end > time)

    (F, result ++ result_prime)
  }

  def getPartials(F: List[Disk], cell: Envelope): List[Disk] = {
      F.filter{ f =>
        if(f.locations.size > 1)
          !cell.contains(f.locations.last) || !cell.contains(f.locations.head)
        else
          !cell.contains(f.locations.head)
      }
  }

  def getEnvelope(dataset: RDD[(Int, Point)]): Envelope = {
    val Xs = dataset.map(_._2.getX).cache
    val Ys = dataset.map(_._2.getY).cache

    val minX = Xs.min()
    val maxX = Xs.max()
    val minY = Ys.min()
    val maxY = Ys.max()

    new Envelope(minX, maxX, minY, maxY)
  }
}