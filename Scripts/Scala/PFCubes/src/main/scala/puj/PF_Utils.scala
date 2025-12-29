package puj

import archery.RTree

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom.{Envelope, GeometryFactory, Point}
import org.locationtech.jts.index.strtree.STRtree

import edu.ucr.dblab.pflock.sedona.quadtree._

import scala.annotation.tailrec
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

import puj.Utils._
import puj.partitioning.{Cell, Cube}
import puj.bfe._
import puj.psi._

object PF_Utils extends Logging {

  def mca(l1: String, l2: String): String = {
    val i = l1.zip(l2).map { case (a, b) => a == b }.indexOf(false)
    l1.substring(0, i)
  }

  @tailrec
  def pruneM(M: List[Disk], M_prime: List[Disk]): List[Disk] = {
    M match {
      case new_flock :: tail =>
        var stop = false
        for { old_flock <- tail if !stop } {
          val count = old_flock.pidsSet.intersect(new_flock.pidsSet).size

          count match {
            case count if new_flock.pids.size == count =>
              if (old_flock.start <= new_flock.start) {
                stop = true // new is contained by a previous old...
              } else if (old_flock.pids.size > count) {
                // old and new do not have the same points.  We iterate next...
              } else {
                old_flock.subset = true // old is a subset of the new one.  We need to remove it...
              }
            case count if old_flock.pids.size == count =>
              if (old_flock.start < new_flock.start) {
                // old is not a subset of new one...
              } else {
                old_flock.subset = true // old is a subset of the new one.  We need to remove it...
              }
            case _ =>
            // old and new have different points.  We iterate next...
          }
        }

        if (!stop)
          pruneM(tail, M_prime :+ new_flock)
        else
          pruneM(tail, M_prime)

      case Nil => M_prime
    }
  }

  @tailrec
  def pruneN(M: List[Disk], N: List[Disk], N_prime: List[Disk]): List[Disk] = {
    N match {
      case n :: tail =>
        if (M.exists(m => n.pids.size == n.pidsSet.intersect(m.pidsSet).size)) {
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
      case flock1 :: tail =>
        var stop = false
        for { flock2 <- tail if !stop } {
          val count = flock2.pidsSet.intersect(flock1.pidsSet).size

          count match {
            case count if flock1.pids.size == count =>
              if (flock2.start <= flock1.start && flock1.end <= flock2.end) {
                stop = true // flock1 is contained by flock2...
              } else if (flock2.pids.size > count) {
                // flock1 and flock2 do not have the same points.  We iterate next...
              } else {
                if (flock2.start < flock1.start || flock1.end < flock2.end) {
                  // flock2 is not a subset of flock1 (they have same pids but differ in time).  We iterate next..
                } else {
                  flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
                }
              }
            case count if flock2.pids.size == count =>
              if (flock2.start < flock1.start || flock1.end < flock2.end) {
                // flock2 is not a subset of flock1.  We iterate next..
              } else {
                flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
              }
            case _ =>
            // flock1 and flock2 have different points.  We iterate next...
          }
        }

        if (!stop)
          pruneQ(tail, Q_prime :+ flock1)
        else
          pruneQ(tail, Q_prime)

      case Nil => Q_prime.filterNot(_.subset)
    }
  }

  def pruneP(P: List[Disk]): List[Disk] = pruneQ(P, List.empty[Disk])

  def parPrune(flocks: List[Disk])(implicit spark: SparkSession, S: Settings, cubes: Map[Int, Cube], tree: StandardQuadTree[Point]): List[Disk] = {
    val n = tree.getLeafZones.size()
    spark.sparkContext
      .parallelize(flocks)
      .flatMap { flock =>
        val envelope = flock.getExpandEnvelope(S.epsilon)
        tree.findZones(new QuadRectangle(envelope)).asScala.map { zone =>
          (zone.partitionId.toInt, flock)
        }
      }
      .partitionBy(SimplePartitioner(n))
      .mapPartitionsWithIndex { (index, it) =>
        val t0     = clocktime
        val cell   = cubes(index).cell
        val flocks = it.map(_._2).toList
        val r      = pruneByArchery(flocks).filter { f =>
          cell.contains(f)
        }.toIterator
        val tParPrune = (clocktime - t0) / 1e9
        debug {
          logger.info(s"TIME|PER_CELL|ParPrune|$index|$tParPrune")
        }

        r
      }
      .collect()
      .toList
  }

  def pruneByArchery(flocks: List[Disk])(implicit S: Settings): List[Disk] = {
    var tree = archery.RTree[Disk]()
    val f    = flocks.sortBy(_.start).zipWithIndex.map { case (flock, id) =>
      flock.id = id
      tree = tree.insert(flock.pointEntry)

      flock
    }

    pruneByArcheryRec(f, List.empty[Disk], tree, S)
  }
  def pruneByLocation(flocks: List[Disk])(implicit S: Settings): List[Disk] = {
    val tree = new STRtree()
    val f    = flocks.sortBy(_.start).zipWithIndex.map { case (flock, id) =>
      flock.id = id
      tree.insert(flock.envelope, flock)

      flock
    }

    pruneByLocationRec(f, List.empty[Disk], tree, S)
  }

  def pruneByLocation(flocks1: List[Disk], flocks2: List[Disk])(implicit S: Settings): List[Disk] = {
    val tree = new STRtree()
    flocks2.foreach { flock =>
      tree.insert(flock.envelope, flock)
    }

    pruneByLocationRec(flocks1, List.empty[Disk], tree, S)
  }
  @tailrec
  def pruneByLocationRec(Q: List[Disk], Q_prime: List[Disk], tree: STRtree, S: Settings): List[Disk] = {
    Q match {
      case flock1 :: tail =>
        var stop = false
        val zone = tree.query(flock1.getExpandEnvelope(S.epsilon)).asScala.map(_.asInstanceOf[Disk])
        for { flock2 <- zone if !stop } {
          val count = flock2.pidsSet.intersect(flock1.pidsSet).size

          count match {
            case count if flock1.pids.size == count =>
              if (flock2.start <= flock1.start && flock1.end <= flock2.end) {
                stop = true // flock1 is contained by flock2...
              } else if (flock2.pids.size > count) {
                // flock1 and flock2 do not have the same points.  We iterate next...
              } else {
                if (flock2.start < flock1.start || flock1.end < flock2.end) {
                  // flock2 is not a subset of flock1 (they have same pids but differ in time).  We iterate next..
                } else {
                  flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
                }
              }
            case count if flock2.pids.size == count =>
              if (flock2.start < flock1.start || flock1.end < flock2.end) {
                // flock2 is not a subset of flock1.  We iterate next..
              } else {
                flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
              }
            case _ =>
            // flock1 and flock2 have different points.  We iterate next...
          }
        }

        if (!stop)
          pruneByLocationRec(tail, Q_prime :+ flock1, tree, S)
        else
          pruneByLocationRec(tail, Q_prime, tree, S)

      case Nil => Q_prime
    }
  }

  @tailrec
  private def pruneByArcheryRec(Q: List[Disk], Q_prime: List[Disk], tree_prime: RTree[Disk], S: Settings): List[Disk] = {
    tree_prime.entries.toList match {
      case flock_entry :: tail =>
        var stop   = false
        val flock1 = flock_entry.value
        var tree   = tree_prime.remove(flock_entry)
        val zone   = tree.search(flock1.bbox(S.epsilon.toFloat)).toList
        for { flock2_entry <- zone if !stop } {
          val flock2 = flock2_entry.value
          val count  = flock2.pidsSet.intersect(flock1.pidsSet).size

          tree = count match {
            case count if flock1.pids.size == count =>
              if (flock2.start <= flock1.start && flock1.end <= flock2.end) {
                stop = true // flock1 is contained by flock2...
                tree
              } else if (flock2.pids.size > count) {
                // flock1 and flock2 do not have the same points.  We iterate next...
                tree
              } else {
                if (flock2.start < flock1.start || flock1.end < flock2.end) {
                  // flock2 is not a subset of flock1 (they have same pids but differ in time).  We iterate next..
                  tree
                } else {
                  flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
                  tree.remove(flock2_entry)
                }
              }
            case count if flock2.pids.size == count =>
              if (flock2.start < flock1.start || flock1.end < flock2.end) {
                // flock2 is not a subset of flock1.  We iterate next..
                tree
              } else {
                flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
                tree.remove(flock2_entry)
              }
            case _ =>
              // flock1 and flock2 have different points.  We iterate next...
              tree
          }

        }

        if (!stop)
          pruneByArcheryRec(List.empty[Disk], Q_prime :+ flock1, tree, S)
        else
          pruneByArcheryRec(List.empty[Disk], Q_prime, tree, S)

      case Nil => Q_prime
    }
  }
  @tailrec
  private def pruneByArcheryRec2(Q: List[Disk], Q_prime: List[Disk], tree_prime: RTree[Disk], S: Settings): List[Disk] = {
    tree_prime.entries.toList match {
      case flock_entry :: tail =>
        var stop   = false
        var flock1 = flock_entry.value
        var tree   = tree_prime.remove(flock_entry)
        val zone   = tree.search(flock1.bbox(S.epsilon.toFloat)).toList
        for { flock2_entry <- zone if !stop } {
          val flock2 = flock2_entry.value
          val count  = flock2.pidsSet.intersect(flock1.pidsSet).size

          tree = if (flock1.start == flock2.start && flock1.end == flock2.end && flock1.pidsText == flock2.pidsText) {
            stop = true
            if (flock1 < flock2) {
              tree
            } else {
              flock1 = flock2
              tree.remove(flock2_entry)
            }
          } else {
            count match {
              case count if flock1.pids.size == count =>
                if (flock2.start <= flock1.start && flock1.end <= flock2.end) {
                  stop = true // flock1 is contained by flock2...
                  tree
                } else if (flock2.pids.size > count) {
                  // flock1 and flock2 do not have the same points.  We iterate next...
                  tree
                } else {
                  if (flock2.start < flock1.start || flock1.end < flock2.end) {
                    // flock2 is not a subset of flock1 (they have same pids but differ in time).  We iterate next..
                    tree
                  } else {
                    flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
                    tree.remove(flock2_entry)
                  }
                }
              case count if flock2.pids.size == count =>
                if (flock2.start < flock1.start || flock1.end < flock2.end) {
                  // flock2 is not a subset of flock1.  We iterate next..
                  tree
                } else {
                  flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
                  tree.remove(flock2_entry)
                }
              case _ =>
                // flock1 and flock2 have different points.  We iterate next...
                tree
            }
          }
        }

        if (!stop)
          pruneByArcheryRec2(List.empty[Disk], Q_prime :+ flock1, tree, S)
        else
          pruneByArcheryRec2(List.empty[Disk], Q_prime, tree, S)

      case Nil => Q_prime
    }
  }

  def prune2(Q: List[Disk], R: List[Disk], Q_prime: List[Disk]): List[Disk] = {
    Q match {
      case flock1 :: tail =>
        var stop = false
        for { flock2 <- R if !stop } {
          val count = flock2.pidsSet.intersect(flock1.pidsSet).size

          count match {
            case count if flock1.pids.size == count =>
              if (flock2.start <= flock1.start && flock1.end <= flock2.end) {
                stop = true // flock1 is contained by flock2...
              } else if (flock2.pids.size > count) {
                // flock1 and flock2 do not have the same points.  We iterate next...
              } else {
                if (flock2.start < flock1.start || flock1.end < flock2.end) {
                  // flock2 is not a subset of flock1 (they have same pids but differ in time).  We iterate next..
                } else {
                  flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
                }
              }
            case count if flock2.pids.size == count =>
              if (flock2.start < flock1.start || flock1.end < flock2.end) {
                // flock2 is not a subset of flock1.  We iterate next..
              } else {
                flock2.subset = true // flock2 is a subset of flock1.  We need to remove it...
              }
            case _ =>
            // flock1 and flock2 have different points.  We iterate next...
          }
        }

        if (!stop)
          prune2(tail, R.filterNot(_.subset), Q_prime :+ flock1)
        else
          prune2(tail, R.filterNot(_.subset), Q_prime)

      case Nil => Q_prime ++ R
    }
  }

  @tailrec
  def join(trajs: List[(Int, Iterable[STPoint])], flocks: List[Disk], f: List[Disk])(implicit S: Settings, P: Params, G: GeometryFactory): List[Disk] = {

    trajs match {
      case current_trajs :: remaining_trajs =>
        val time   = current_trajs._1
        val points = current_trajs._2.toList

        val (new_flocks, stats) =
          if (S.method == "BFE")
            BFE.run(points)
          else {
            PSI.run(points)

          }
        // stats.printPSI()
        debug {
          stats.printPSI()
        }

        /** * start: merging previous flocks with current flocks...
          */
        val merged_ones = if (S.method != "BFE") {
          val inverted_index = flocks
            .flatMap { flock =>
              flock.pids.map { pid =>
                pid -> flock
              }
            }
            .groupBy(_._1)
            .mapValues {
              _.map(_._2)
            }

          val flocks_prime = for { new_flock <- new_flocks } yield {
            val disks = new_flock.pids
              .filter { pid => inverted_index.keySet.contains(pid) }
              .flatMap { pid =>
                inverted_index(pid)
              }
              .distinct

            disks
              .map { old_flock =>
                val pids  = old_flock.pidsSet.intersect(new_flock.pidsSet).toList
                val flock = Disk(new_flock.center, pids, old_flock.start, time)
                flock.locations = old_flock.locations :+ new_flock.center.getCoordinate

                if (pids == new_flock.pids) new_flock.subset = true

                flock
              }
              .filter(_.pids.size >= S.mu) // filtering by minimum number of entities (mu)...

          }

          flocks_prime.flatten
        } else {
          val merged_ones = (for {
            old_flock <- flocks
            new_flock <- new_flocks
          } yield {
            val pids  = old_flock.pidsSet.intersect(new_flock.pidsSet).toList
            val flock = Disk(new_flock.center, pids, old_flock.start, time)
            flock.locations = old_flock.locations :+ new_flock.center.getCoordinate

            if (pids == new_flock.pids) new_flock.subset = true

            flock
          }).filter(_.pids.size >= S.mu) // filtering by minimum number of entities (mu)...

          merged_ones
        }

        /** * end: merging previous flocks with current flocks...
          */

        /** * start: pruning subset flocks...
          */
        val M = pruneM(merged_ones, List.empty[Disk]).filterNot(_.subset)

        val N = new_flocks.filterNot(_.subset).map { flock =>
          Disk(flock.center, flock.pids, time, time)
        }

        val candidates = M ++ pruneN(M, N, List.empty[Disk])

        /** * end: pruning subset flocks...
          */

        /** * start: reporting...
          */
        val F_prime = candidates
          .map { flock =>
            val a = flock.end - flock.start
            val b = S.delta - 1

            (flock, a >= b)
          }
        // val count = n + F_prime.count(_._2)
        val r = F_prime.filter(_._2).map(_._1)
        if (S.print) {
          r.foreach { println }
        }

        /** * end: reporting...
          */

        /** * start: recurse...
          */
        val F = F_prime
          .map { case (flock, mustUpdate) =>
            if (mustUpdate) flock.copy(start = flock.start + 1) else flock
          }

        join(remaining_trajs, F, f ++ r)

      /** * end: recurse...
        */
      case Nil => f
    }
  }

  @tailrec
  def joinDisks(trajs: List[(Int, Iterable[STPoint])], flocks: List[Disk], f: List[Disk], cell: Envelope, cell_prime: Envelope, partial: List[Disk])(implicit S: Settings, G: GeometryFactory, P: Params): (List[Disk], List[Disk]) = {
    val pid = TaskContext.getPartitionId()

    trajs match {
      case current_trajs :: remaining_trajs =>
        val time   = current_trajs._1
        val points = current_trajs._2.toList

        val (new_flocks, stats) = if (S.method == "BFE") {
          val (nf, stats) = BFE.run(points)
          (nf.map(_.copy(start = time, end = time)).filter(d => cell_prime.contains(d.center.getCoordinate)), stats)
          // (nf.map(_.copy(start = time, end = time)), stats)
        } else {
          val (nf, stats) = PSI.run(points)
          (nf.map(_.copy(start = time, end = time)).filter(d => cell_prime.contains(d.center.getCoordinate)), stats)
          // (nf.map(_.copy(start = time, end = time)), stats)
        }

        debug {
          stats.printPSI()
        }

        /** * start: merging previous flocks with current flocks...
          */
        val merged_ones = if (S.method != "BFE") {
          val inverted_index = flocks
            .flatMap { flock =>
              flock.pids.map { pid =>
                pid -> flock
              }
            }
            .groupBy(_._1)
            .mapValues {
              _.map(_._2)
            }

          val flocks_prime = for { new_flock <- new_flocks } yield {
            val disks = new_flock.pids
              .filter { pid => inverted_index.keySet.contains(pid) }
              .flatMap { pid =>
                inverted_index(pid)
              }
              .distinct

            disks
              .map { old_flock =>
                val pids  = old_flock.pidsSet.intersect(new_flock.pidsSet).toList
                val flock = Disk(new_flock.center, pids, old_flock.start, time)
                flock.locations = old_flock.locations :+ new_flock.center.getCoordinate

                if (pids == new_flock.pids) new_flock.subset = true

                flock
              }
              .filter(_.pids.size >= S.mu) // filtering by minimum number of entities (mu)...

          }

          flocks_prime.flatten

          // flocks
        } else {
          // flocks
          val merged_ones = (for {
            old_flock <- flocks
            new_flock <- new_flocks
          } yield {
            val pids  = old_flock.pidsSet.intersect(new_flock.pidsSet).toList
            val flock = Disk(new_flock.center, pids, old_flock.start, time)
            flock.locations = old_flock.locations :+ new_flock.center.getCoordinate

            if (pids == new_flock.pids) new_flock.subset = true

            flock
          }).filter(_.pids.size >= S.mu) // filtering by minimum number of entities (mu)...

          merged_ones
        }

        /** * end: merging previous flocks with current flocks...
          */

        /** * start: pruning subset flocks...
          */
        val M = pruneM(merged_ones, List.empty[Disk]).filterNot(_.subset)

        val N = new_flocks.filterNot(_.subset).map { flock =>
          Disk(flock.center, flock.pids, time, time)
        }

        val candidates = M ++ pruneN(M, N, List.empty[Disk])

        /** * end: pruning subset flocks...
          */

        /** * start: reporting...
          */
        val F_prime = candidates
          .map { flock =>
            val safe_delta = flock.end - flock.start >= S.delta - 1
            val safe_area  = cell.contains(flock.locations.head) || cell.contains(flock.locations.last) // report if flock in in safe area

            (flock, safe_delta, safe_area)
          }

        val r = F_prime.filter(f => f._2 && f._3).map(_._1)
        if (S.print) {
          r.foreach { println }
        }

        /** * end: reporting...
          */

        /** * start: recurse...
          */

        val F = F_prime
          .map { case (flock, safe_delta, _) =>
            if (safe_delta) {
              val f = flock.copy(start = flock.start + 1)
              f.locations = flock.locations.tail
              f
            } else flock
          }

        // val F_partial = getPartials(F, cell)
        val F_partial = F_prime.filterNot(_._3).map(_._1)

        joinDisks(remaining_trajs, F, f ++ r, cell, cell_prime, partial ++ F_partial)

      /** * start: recurse...
        */
      case Nil => (f, partial)
    }
  }

  @tailrec
  def joinDisksCachingPartials(
      trajs: List[(Int, Iterable[STPoint])],
      flocks: List[Disk],
      f: List[Disk],
      cell: Envelope,
      cell_prime: Envelope,
      partial: List[Disk],
      time_start: Int,
      time_end: Int,
      partial2: List[Disk],
      partial3: List[Disk]
  )(implicit S: Settings, G: GeometryFactory, C: Map[Int, Cube]): (List[Disk], List[Disk], List[Disk]) = {

    val pid = TaskContext.getPartitionId()
    trajs match {
      case current_trajs :: remaining_trajs =>
        val time   = current_trajs._1
        val points = current_trajs._2.toList

        val (new_flocks, stats) = if (S.method == "BFE") {
          val (nf, stats) = BFE.run(points)
          (nf.map(_.copy(start = time, end = time)).filter(d => cell_prime.contains(d.center.getCoordinate)), stats)
        } else {
          val (nf, stats) = PSI.run(points)
          (nf.map(_.copy(start = time, end = time)).filter(d => cell_prime.contains(d.center.getCoordinate)), stats)
        }

        debug {
          stats.printPSI()
        }

        /** * start: merging previous flocks with current flocks...
          */
        val merged_ones = if (S.method != "BFE") {
          val inverted_index = flocks
            .flatMap { flock =>
              flock.pids.map { pid =>
                pid -> flock
              }
            }
            .groupBy(_._1)
            .mapValues {
              _.map(_._2)
            }

          val flocks_prime = for { new_flock <- new_flocks } yield {
            val disks = new_flock.pids
              .filter { pid => inverted_index.keySet.contains(pid) }
              .flatMap { pid =>
                inverted_index(pid)
              }
              .distinct

            disks
              .map { old_flock =>
                val pids  = old_flock.pidsSet.intersect(new_flock.pidsSet).toList
                val flock = Disk(new_flock.center, pids, old_flock.start, time)
                flock.locations = old_flock.locations :+ new_flock.center.getCoordinate

                if (pids == new_flock.pids) new_flock.subset = true

                flock
              }
              .filter(_.pids.size >= S.mu) // filtering by minimum number of entities (mu)...

          }

          flocks_prime.flatten
        } else { // PSI
          {
            for {
              old_flock <- flocks
              new_flock <- new_flocks
            } yield {
              val pids  = old_flock.pidsSet.intersect(new_flock.pidsSet).toList
              val flock = Disk(new_flock.center, pids, old_flock.start, time)
              flock.locations = old_flock.locations :+ new_flock.center.getCoordinate

              if (pids == new_flock.pids) new_flock.subset = true

              flock
            }
          }.filter(_.pids.size >= S.mu) // filtering by minimum number of entities (mu)...
        }

        /** * end: merging previous flocks with current flocks...
          */

        /** * start: pruning subset flocks...
          */
        val M = pruneM(merged_ones, List.empty[Disk]).filterNot(_.subset)

        val N = new_flocks.filterNot(_.subset).map { flock =>
          Disk(flock.center, flock.pids, time, time)
        }

        val candidates = M ++ pruneN(M, N, List.empty[Disk])

        /** * end: pruning subset flocks...
          */

        /** * start: reporting...
          */
        val F_prime = candidates
          .map { flock =>
            val safe_delta = flock.end - flock.start >= S.delta - 1
            val safe_area  = cell.contains(flock.locations.head) || cell.contains(flock.locations.last) // report if flock is in safe area...

            (flock, safe_delta, safe_area)
          }

        val r = F_prime.filter(f => f._2 && f._3).map(_._1)
        if (S.print) {
          r.foreach { println }
        }

        /** * end: reporting...
          */

        /** * start: recurse...
          */

        val F = F_prime
          .map { case (flock, safe_delta, _) =>
            if (safe_delta) {
              val f = flock.copy(start = flock.start + 1)
              f.locations = flock.locations.tail
              f
            } else flock
          }

        val F_partial = F_prime.filterNot(_._3).map(_._1)

        // Getting partial flocks in time partitions...
        val T_partial_start = if (time < time_start + (S.delta - 1)) {
          val prime = candidates.filter { candidate => candidate.start == time_start }
          PF_Utils.pruneByArchery(prime ++ new_flocks ++ partial2)
        } else {
          partial2
        }
        val T_partial_end = if (time > time_end - (S.delta - 1)) {
          val prime = candidates.filter { candidate => candidate.end == time }.map { f =>
            if (f.start < time) {
              val f_prime = f.copy(start = time)
              f_prime.did = f.did
              f_prime.locations = f.locations
              f_prime
            } else {
              f
            }
          }
          PF_Utils.pruneByArchery(prime ++ partial3)
        } else {
          partial3
        }

        // Recursion...
        joinDisksCachingPartials(
          remaining_trajs,
          F,
          f ++ r,
          cell,
          cell_prime,
          partial ++ F_partial,
          time_start,
          time_end,
          T_partial_start,
          T_partial_end
        )

      /** * start: recurse...
        */

      case Nil => (f, partial, partial2 ++ partial3)
    }
  }

  def joinDisks2(trajs: List[(Int, Iterable[STPoint])], flocks: List[Disk], f: List[Disk], cell: Envelope, cell_prime: Envelope, partial: List[Disk])(implicit S: Settings, G: GeometryFactory, P: Params): (List[Disk], List[Disk]) = {
    val pid = TaskContext.getPartitionId()

    trajs match {
      case current_trajs :: remaining_trajs =>
        val time   = current_trajs._1
        val points = current_trajs._2.toList

        val (new_flocks, stats) = if (S.method == "BFE") {
          val (nf, stats) = BFE.run(points)
          (nf.map(_.copy(start = time, end = time)).filter(d => cell_prime.contains(d.center.getCoordinate)), stats)
          // (nf.map(_.copy(start = time, end = time)), stats)
        } else {
          val (nf, stats) = PSI.run(points)
          (nf.map(_.copy(start = time, end = time)).filter(d => cell_prime.contains(d.center.getCoordinate)), stats)
          // (nf.map(_.copy(start = time, end = time)), stats)
        }

        debug {
          stats.printPSI()
        }

        /** * start: merging previous flocks with current flocks...
          */
        val merged_ones = if (S.method != "BFE") {
          val inverted_index = flocks
            .flatMap { flock =>
              flock.pids.map { pid =>
                pid -> flock
              }
            }
            .groupBy(_._1)
            .mapValues {
              _.map(_._2)
            }

          val flocks_prime = for { new_flock <- new_flocks } yield {
            val disks = new_flock.pids
              .filter { pid => inverted_index.keySet.contains(pid) }
              .flatMap { pid =>
                inverted_index(pid)
              }
              .distinct

            disks
              .map { old_flock =>
                val pids  = old_flock.pidsSet.intersect(new_flock.pidsSet).toList
                val flock = Disk(new_flock.center, pids, old_flock.start, time)
                flock.locations = old_flock.locations :+ new_flock.center.getCoordinate

                if (pids == new_flock.pids) new_flock.subset = true

                flock
              }
              .filter(_.pids.size >= S.mu) // filtering by minimum number of entities (mu)...

          }

          flocks_prime.flatten

          // flocks
        } else {
          // flocks
          val merged_ones = (for {
            old_flock <- flocks
            new_flock <- new_flocks
          } yield {
            val pids  = old_flock.pidsSet.intersect(new_flock.pidsSet).toList
            val flock = Disk(new_flock.center, pids, old_flock.start, time)
            flock.locations = old_flock.locations :+ new_flock.center.getCoordinate

            if (pids == new_flock.pids) new_flock.subset = true

            flock
          }).filter(_.pids.size >= S.mu) // filtering by minimum number of entities (mu)...

          merged_ones
        }

        /** * end: merging previous flocks with current flocks...
          */

        /** * start: pruning subset flocks...
          */
        val M = pruneM(merged_ones, List.empty[Disk]).filterNot(_.subset)

        val N = new_flocks.filterNot(_.subset).map { flock =>
          Disk(flock.center, flock.pids, time, time)
        }

        val candidates = M ++ pruneN(M, N, List.empty[Disk])

        /** * end: pruning subset flocks...
          */

        /** * start: reporting...
          */
        val F_prime = candidates
          .map { flock =>
            val safe_delta = flock.end - flock.start >= S.delta - 1
            val safe_area  = flock.locations.exists(c => cell.contains(c)) // report if a flock location is in safe area

            (flock, safe_delta, safe_area)
          }

        val r = F_prime.filter(f => f._2).map(_._1)
        if (S.print) {
          r.foreach { println }
        }

        /** * end: reporting...
          */

        /** * start: recurse...
          */

        val F = F_prime
          .map { case (flock, safe_delta, _) =>
            if (safe_delta) {
              val f = flock.copy(start = flock.start + 1)
              f.locations = flock.locations.tail
              f
            } else flock
          }

        val F_partial = new_flocks.filterNot(f => cell.contains(f.center.getCoordinate))

        joinDisks2(remaining_trajs, F, f ++ r, cell, cell_prime, partial ++ F_partial)

      /** * start: recurse...
        */
      case Nil => (f, partial)
    }
  }

  def funPartial(f: List[Disk], time: Int, partials: mutable.HashMap[Int, (List[Disk], STRtree)], result: List[Disk])(implicit S: Settings, G: GeometryFactory): (List[Disk], List[Disk]) = {
    val pid = TaskContext.getPartitionId()

    val (seeds, _) =
      try {
        partials(time)
      } catch {
        case e: Exception => (List.empty[Disk], new STRtree())
      }

    val t1     = clocktime
    val flocks = f ++ pruneByArchery(seeds)
    // println(s"prune 1: ${(clocktime - t1)/1e9}")

    val t2 = clocktime
    val D  = for (flock1 <- flocks) yield {

      val (_, tree) =
        try {
          partials(flock1.end + 1)
        } catch {
          case e: Exception => (List.empty[Disk], new STRtree())
        }

      val zone = flock1.getExpandEnvelope(S.sdist + S.tolerance)
      tree
        .query(zone)
        .asScala
        .map { _.asInstanceOf[Disk] }
        .filter { flock2 =>
          flock1.pidsSet.intersect(flock2.pidsSet).size >= S.mu
        }
        .map { flock2 =>
          val pids  = flock1.pidsSet.intersect(flock2.pidsSet).toList
          val start = flock1.start
          val end   = flock2.end

          val d = Disk(flock2.center, pids, start, end)
          d.locations = flock1.locations ++ flock2.locations
          d.did = flock2.did
          d
        }
    }
    // println(s"merge: ${(clocktime - t2)/1e9}")

    val t3 = clocktime
    // val candidates = pruneP(flocks ++ D.flatten)
    val D_prime = D.flatMap { d =>
      pruneByArchery(d.toList)
    }
    val candidates = pruneByArchery(flocks) ++ D_prime
    // println(s"prune 2: ${(clocktime - t3)/1e9}")

    val t4      = clocktime
    val F_prime = candidates.map { f =>
      val a = f.start == time - (S.delta - 1)
      (f, a)
    }
    val r = F_prime.filter(_._2).map { case (f, _) =>
      val loc     = f.locations.slice(0, S.delta - 1)
      val f_prime = f.copy(center = G.createPoint(loc.last), end = time)
      f_prime.locations = loc
      f_prime
    }
    // println(s"reporting: ${(clocktime - t4)/1e9}")

    val t5           = clocktime
    val result_prime = pruneByArchery(r)
    // println(s"Time: $time \t ${r.size} \t ${result_prime.size}")
    // println(s"prune 3: ${(clocktime - t5)/1e9}")

    val t6 = clocktime
    val F  = F_prime
      .map { case (flock, mustUpdate) =>
        if (mustUpdate) {
          val f = flock.copy(start = flock.start + 1)
          f.locations = flock.locations.tail
          f.did = flock.did
          f
        } else flock
      }
      .filter(_.end > time)
    // println(s"update: ${(clocktime - t6)/1e9}")

    (F, result ++ result_prime)
  }

  def funPartial2(f: List[Disk], time: Int, partials: mutable.HashMap[Int, (List[Disk], STRtree)], result: List[Disk])(implicit S: Settings): (List[Disk], List[Disk]) = {
    val pid = TaskContext.getPartitionId()

    val new_flocks  = partials(time)._1
    val flocks      = f
    val merged_ones = if (S.method != "BFE") {
      val inverted_index = flocks
        .flatMap { flock =>
          flock.pids.map { pid =>
            pid -> flock
          }
        }
        .groupBy(_._1)
        .mapValues {
          _.map(_._2)
        }

      val flocks_prime = for { new_flock <- new_flocks } yield {
        val disks = new_flock.pids
          .filter { pid => inverted_index.keySet.contains(pid) }
          .flatMap { pid =>
            inverted_index(pid)
          }
          .distinct

        disks
          .map { old_flock =>
            val pids  = old_flock.pidsSet.intersect(new_flock.pidsSet).toList
            val flock = Disk(new_flock.center, pids, old_flock.start, time)
            flock.locations = old_flock.locations :+ new_flock.center.getCoordinate
            flock.dids = old_flock.dids :+ new_flock.did

            if (pids == new_flock.pids) new_flock.subset = true

            flock
          }
          .filter(_.pids.size >= S.mu) // filtering by minimum number of entities (mu)...

      }

      flocks_prime.flatten
    } else {
      val merged_ones = (for {
        old_flock <- flocks
        new_flock <- new_flocks
      } yield {
        val pids  = old_flock.pidsSet.intersect(new_flock.pidsSet).toList
        val flock = Disk(new_flock.center, pids, old_flock.start, time)
        flock.locations = old_flock.locations :+ new_flock.center.getCoordinate
        flock.dids = old_flock.dids :+ new_flock.did

        if (pids == new_flock.pids) new_flock.subset = true

        flock
      }).filter(_.pids.size >= S.mu) // filtering by minimum number of entities (mu)...

      merged_ones
    }

    /** * end: merging previous flocks with current flocks...
      */

    /** * start: pruning subset flocks...
      */
    val M = pruneM(merged_ones, List.empty[Disk]).filterNot(_.subset)

    val N = new_flocks.filterNot(_.subset).map { flock =>
      Disk(flock.center, flock.pids, time, time)
    }

    val candidates = M ++ pruneN(M, N, List.empty[Disk])

    /** * end: pruning subset flocks...
      */

    /** * start: reporting...
      */
    val F_prime = candidates
      .map { flock =>
        val a                  = flock.end - flock.start
        val b                  = S.delta - 1
        val same_cell: Boolean = !flock.dids.zip(flock.dids.tail).forall { case (a, b) => a == b }

        (flock, a >= b, same_cell)
      }
    // val count = n + F_prime.count(_._2)
    val r = F_prime.filter(f => f._2).map(_._1)
    if (S.print) {
      r.foreach { println }
    }

    /** * end: reporting...
      */

    /** * start: recurse...
      */
    val F = F_prime
      .map { case (flock, mustUpdate, _) =>
        if (mustUpdate) {
          val f = flock.copy(start = flock.start + 1)
          f.locations = flock.locations.tail
          f.did = flock.did
          f.dids = flock.dids.tail
          f
        } else flock
      }

    (F, result ++ r)
  }

  def processUpdates(flocksRDD: RDD[Disk], cells: Map[Int, Cell], n: Int = 1, i: Int = 1)(implicit S: Settings, G: GeometryFactory, P: Params): (RDD[Disk], Map[Int, Cell]) = {

    // var t0 = clocktime
    val cellsA = PF_Utils.updateCells(cells, n)
    // var t1 = (clocktime - t0) / 1e9
    val ncells = cellsA.size
    // logt(s"PROCESS|$ncells|$i|data|$t1")

    save(s"/tmp/edgesC${i}.wkt") {
      cellsA.values.map { cell =>
        s"${cell.wkt}\n"
      }.toList
    }

    // t0 = clocktime
    val treeA = PF_Utils.buildTree(cellsA)
    // t1 = (clocktime - t0) / 1e9
    // logt(s"PROCESS|$ncells|$i|tree|$t1")

    // t0 = clocktime
    val ARDD = flocksRDD
      .mapPartitionsWithIndex { (index, flocks) =>
        val env    = new Envelope(cells(index).envelope.centre())
        val new_id = treeA.query(env).asScala.map(_.asInstanceOf[Int]).head
        flocks.map(f => (new_id, f))
      }
      .partitionBy(SimplePartitioner(ncells))
      .map(_._2)
      .cache
    ARDD.count
    // t1 = (clocktime - t0) / 1e9
    // logt(s"PROCESS|$ncells|$i|part|$t1")

    // t0 = clocktime
    val B = ARDD.mapPartitionsWithIndex { (index, F_prime) =>
      val F = F_prime.toList
      // var q0 = clocktime
      val (flocks, partials_prime) = F.partition(_.did == -1)
      val C                        = new Envelope(cellsA(index).envelope)
      C.expandBy(S.sdist * -1.0)
      val (p_prime, still_partials) = partials_prime.partition(f => C.contains(f.center.getCoordinate))
      // var q1 = (clocktime - q0) / 1e9
      // logt(s"STEP|$ncells|$i|$index|data|$q1")

      /*
      val f1 = flocks.toList
      save(s"/tmp/edgesF_C${i}_I${index}.wkt"){
        f1.map{ f =>
          s"${f.wkt}\n"
        }
      }
      val p1 = p_prime.toList
      save(s"/tmp/edgesP_C${i}_I${index}.wkt"){
        p1.map{ f =>
          s"${f.wkt}\n"
        }
      }
      val s1 = still_partials.toList
      save(s"/tmp/edgesS_C${i}_I${index}.wkt"){
        s1.map{ f =>
          s"${f.wkt}\n"
        }
      }*/
      // log(s"STEP|$ncells|$i|$index|flocks|${f1.size}")
      // log(s"STEP|$ncells|$i|$index|Ppartial|${p1.size}")
      // log(s"STEP|$ncells|$i|$index|Spartial|${s1.size}")

      val R = if (p_prime.isEmpty) {
        // logt(s"STEP|$ncells|$i|$index|sort|0.0")
        // logt(s"STEP|$ncells|$i|$index|process|0.0")
        List.empty[Disk]
      } else {
        // q0 = clocktime
        val P        = p_prime.toList.sortBy(_.start).groupBy(_.start)
        val partials = collection.mutable.HashMap[Int, (List[Disk], STRtree)]()
        P.toSeq.map { case (time, candidates_prime) =>
          val candidates = candidates_prime.toList
          val tree       = new STRtree()
          candidates.foreach { candidate =>
            tree.insert(candidate.center.getEnvelopeInternal, candidate)
          }

          partials(time) = (candidates, tree)
        }
        val times = (0 to S.endtime).toList
        // q1 = (clocktime - q0) / 1e9
        // logt(s"STEP|$ncells|$i|$index|sort|$q1")

        // q0 = clocktime
        val R = PF_Utils.processPartials(List.empty[Disk], times, partials, List.empty[Disk])
        R.foreach(r => r.did = -1)
        // q1 = (clocktime - q0) / 1e9
        // logt(s"STEP|$ncells|$i|$index|process|$q1")

        R
      }

      // q0 = clocktime
      val flocks_prime = flocks
      val Q            = (flocks_prime ++ pruneByLocation(R, flocks_prime)).toIterator ++ still_partials
      // q1 = (clocktime - q0) / 1e9
      // logt(s"STEP|$ncells|$i|$index|prune|$q1")

      Q
    }.cache
    B.count
    // t1 = (clocktime - t0) / 1e9
    // logt(s"PROCESS|$ncells|$i|process|$t1")

    (B, cellsA)
  }

  @tailrec
  def processPartials(F: List[Disk], times: List[Int], partials: mutable.HashMap[Int, (List[Disk], STRtree)], R: List[Disk])(implicit S: Settings, G: GeometryFactory): List[Disk] = {
    times match {
      case time :: tail =>
        val (f_prime, r_prime) = PF_Utils.funPartial(F, time, partials, R)
        processPartials(f_prime, tail, partials, r_prime)
      case Nil => R
    }
  }

  @tailrec
  def processPartials2(F: List[Disk], times: List[Int], partials: mutable.HashMap[Int, (List[Disk], STRtree)], R: List[Disk])(implicit S: Settings): List[Disk] = {
    times match {
      case time :: tail =>
        val (f_prime, r_prime) = PF_Utils.funPartial2(F, time, partials, R)
        processPartials2(f_prime, tail, partials, r_prime)
      case Nil => R
    }
  }

  @tailrec
  def process(flocks: RDD[Disk], cells: Map[Int, Cell], n: Int = 1, i: Int = 1)(implicit S: Settings, G: GeometryFactory, P: Params): (RDD[Disk], Map[Int, Cell]) = {
    if (cells.size > 1) {
      val (a, cellsa) = PF_Utils.processUpdates(flocks, cells, n, i)
      process(a, cellsa, n, i + 1)
    } else {
      (flocks, cells)
    }
  }
  def updateCells(cells: Map[Int, Cell], e: Int)(implicit G: GeometryFactory): Map[Int, Cell] = updateCells2(cells, 0, e)

  @tailrec
  private def updateCells2(cells: Map[Int, Cell], i: Int, e: Int)(implicit G: GeometryFactory): Map[Int, Cell] = {
    if (cells.size > 1 && i < e) {
      val cellsA = PF_Utils.updateCellsSec(cells)
      updateCells2(cellsA, i + 1, e)
    } else {
      cells
    }
  }

  private def updateCellsSec(cells: Map[Int, Cell])(implicit G: GeometryFactory): Map[Int, Cell] = {
    cells.values
      .groupBy { cell =>
        val lid = cell.lineage.substring(0, cell.lineage.length - 1)
        lid
      }
      .flatMap { case (lid, cells) =>
        if (cells.size == 4) {
          val env = cells.map(c => new Envelope(c.envelope)).reduce { (a, b) =>
            a.expandToInclude(b)
            a
          }
          List(Cell(-1, env, lid))
        } else {
          cells.toList
        }
      }
      .zipWithIndex
      .map { case (cell, id) => id -> cell.copy(id = id) }
      .toMap
  }

  def buildTree(cells: Map[Int, Cell]): STRtree = {
    val tree = new STRtree()
    cells.values.foreach { cell =>
      tree.insert(cell.envelope, cell.id)
    }
    tree
  }

  def getEnvelope(dataset: RDD[Point]): Envelope = {
    val Xs = dataset.map(_.getX).cache
    val Ys = dataset.map(_.getY).cache

    val minX = Xs.min()
    val maxX = Xs.max()
    val minY = Ys.min()
    val maxY = Ys.max()

    new Envelope(minX, maxX, minY, maxY)
  }

  def getEnvelope2(dataset: RDD[Point]): Envelope = {
    val Xs = dataset.map(_.getX).cache
    val Ys = dataset.map(_.getY).cache

    val minX = Xs.min()
    val maxX = Xs.max()
    val minY = Ys.min()
    val maxY = Ys.max()

    new Envelope(minX, maxX, minY, maxY)
  }

  def getTime(p: Point)(implicit G: GeometryFactory): Int = p.getUserData.asInstanceOf[Data].tid

}
