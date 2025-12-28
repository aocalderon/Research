package puj.psi

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

import archery.{RTree => ArcheryRTree}

import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import puj.psi.PSI_Utils._
import puj.{Setup, Settings}
import puj.Utils._

object PSI extends Logging {

  /** Find candidates disks and active boxes with plane sweeping technique.
    * @param points
    *   the set of points.
    * @return
    *   list of candidates and list of boxes.
    */
  def planeSweeping2(points: List[STPoint])(implicit S: Settings, G: GeometryFactory, stats: Stats): (RTree[Disk], ArcheryRTree[Box]) = {

    // ordering by coordinate (it is first by x and then by y)...
    val pointset: List[STPoint] = points.sortBy(_.getCoord)

    debug {
      save("/tmp/edgesPointsPSI.wkt") {
        pointset.zipWithIndex
          .map { case (point, order) => s"${point.wkt}\t$order\n" }
      }
    }

    // setting data structures to store candidates and boxes...
    val candidates: RTree[Disk]               = RTree[Disk]()
    var boxes: ArcheryRTree[Box]              = ArcheryRTree[Box]()
    var pairs: ListBuffer[(STPoint, STPoint)] = ListBuffer()
    var nPairs                                = 0
    var nCenters                              = 0
    var tBand                                 = 0.0
    var tPairs                                = 0.0
    var tCenters                              = 0.0
    var tCandidates                           = 0.0
    var tBoxes                                = 0.0

    // feeding candidates and active boxes...
    pointset.foreach { pr: STPoint =>
      // feeding band with points inside 2-epsilon x 2-epsilon...
      val band_for_pr: RTree[STPoint] = RTree[STPoint]()
      val (_, tB)                     = timer {
        pointset
          .filter { ps: STPoint =>
            math.abs(ps.X - pr.X) <= S.epsilon && math.abs(ps.Y - pr.Y) <= S.epsilon
          }
          .foreach { ps: STPoint =>
            band_for_pr.put(ps.envelope, ps)
          }
      }
      tBand += tB

      // finding pairs of points, centers, candidates and boxes...
      val (band_pairs, tP) = timer {
        band_for_pr
          .getAll[STPoint]
          .filter { p =>
            p.X >= pr.X &&              // those at the right...
            p.oid != pr.oid &&          // prune duplicates...
            p.distance(pr) <= S.epsilon // getting pairs...
          }
      }
      pairs.appendAll(band_pairs.map(p => (pr, p)))
      nPairs += band_pairs.size
      tPairs += tP

      band_pairs.foreach { p =>
        val (band_centres, tC) = timer {
          computeCentres(pr, p) // gettings centres...
        }
        nCenters += band_centres.size
        tCenters += tC

        band_centres.foreach { centre =>
          val t0       = clocktime
          val envelope = centre.getEnvelopeInternal
          envelope.expandBy(S.r)
          val hood = band_for_pr
            .get[STPoint](envelope)
            .filter { _.distanceToPoint(centre) <= S.r }

          val t1 = if (hood.size >= S.mu) {
            val candidate = Disk(centre, hood.map(_.oid))
            candidates.put(candidate.envelope, candidate) // getting candidates...
            val t1 = clocktime

            val active_box = Box(band_for_pr)

            debug {
              println(active_box.wkt)
            }

            val active_box_hood = boxes.searchIntersection(active_box.boundingBox)

            def coveredBy: Boolean = active_box_hood.exists(b => b.value.covers(active_box))
            def covers: Boolean    = active_box_hood.exists(b => active_box.covers(b.value))
            if (coveredBy) {
              // do nothing...
            } else if (covers) {
              // remove previous box and add the active one...
              val box_prime = active_box_hood.find(b => active_box.covers(b.value)).get
              boxes = boxes.remove(box_prime).insert(active_box.archeryEntry)
            } else {
              // otherwise add the active box...
              boxes = boxes.insert(active_box.archeryEntry)
            }
            t1
          } else clocktime
          val t2 = clocktime
          tCandidates += (t1 - t0)
          tBoxes += (t2 - t1)
        }
      }
    } // foreach pointset
    stats.nPairs = nPairs
    stats.nCenters = nCenters
    stats.nCandidates = candidates.size
    stats.nBoxes = boxes.size
    stats.tBand = tBand
    stats.tPairs = tPairs
    stats.tCenters = tCenters
    stats.tCandidates = tCandidates / 1e9

    debug {
      save("/tmp/edgesPairsPSI.wkt") {
        pairs.toList.map { case (pr, p) =>
          val coords = Array(pr.getCoord, p.getCoord)
          G.createLineString(coords).toText + "\n"
        }
      }
    }

    (candidates, boxes)
  }

  def planeSweeping(points: List[STPoint], time_instant: Int)(implicit S: Settings, G: GeometryFactory, stats: Stats): List[Box] = {

    // ordering by coordinate (it is first by x and then by y)...
    val (pointset, tSort) = timer {
      points.sortBy(_.getCoord)
    }
    stats.tSort = tSort

    // setting data structures to store candidates and boxes...
    var pairs: ListBuffer[(STPoint, STPoint)] = ListBuffer()
    var nPairs                                = 0
    var nCenters                              = 0
    var nCandidates                           = 0
    var tPairs                                = 0.0
    var tCenters                              = 0.0
    var tCandidates                           = 0.0

    // feeding bands with points inside 2-epsilon x 2-epsilon...
    val (bands, tBand) = timer {
      val tree = new STRtree()
      pointset.foreach { P =>
        tree.insert(P.point.getEnvelopeInternal, P)
      }
      pointset
        .map { pr: STPoint =>
          val band_for_pr: RTree[STPoint] = RTree[STPoint]()
          band_for_pr.pr = pr

          val env = new Envelope(pr.envelope)
          env.expandBy(S.epsilon)
          tree.query(env).asScala.map { _.asInstanceOf[STPoint] }.foreach { ps: STPoint =>
            band_for_pr.put(ps.envelope, ps)
          }

          band_for_pr
        }
        .filter(_.size() >= S.mu)
    }
    stats.tBand = tBand

    val t_prime      = clocktime
    val active_boxes = bands.map { band =>
      var candidates: ListBuffer[Disk] = new ListBuffer[Disk]
      val points                       = band.getAll[STPoint]
      val pr                           = band.pr
      val (band_pairs, tP)             = timer {
        points.filter { p =>
          p.X >= pr.X &&              // those at the right...
          p.oid != pr.oid &&          // prune duplicates...
          p.distance(pr) <= S.epsilon // getting pairs...
        }
      }
      debug {
        logger.info(f"pr: $pr\t points: ${points.mkString(" ")}")
      }

      pairs.appendAll(band_pairs.map(p => (pr, p)))
      nPairs += band_pairs.size
      tPairs += tP

      band_pairs.foreach { p =>
        val (band_centres, tC) = timer {
          computeCentres(pr, p) // gettings centres...
        }
        nCenters += band_centres.size
        tCenters += tC

        debug {
          band_centres.map { centre =>
            val wkt = centre.toText
            s"$wkt\t${S.epsilon}"
          } // .foreach{println}
        }

        band_centres.foreach { centre =>
          val t0       = clocktime
          val envelope = centre.getEnvelopeInternal
          envelope.expandBy(S.r)
          val hood = band.get[STPoint](envelope).filter { _.distanceToPoint(centre) <= S.r }

          if (hood.size >= S.mu) {
            // val c = G.createMultiPoint(hood.map(_.point).toArray).getCentroid
            // val candidate = Disk(c, hood.map(_.oid), time_instant, time_instant) // set with the default time instance...
            val candidate = Disk(centre, hood.map(_.oid), time_instant, time_instant) // set with the default time instance...
            candidates.append(candidate) // getting candidates...
          }
          tCandidates += (clocktime - t0) / 1e9
        } // foreach centres
      }   // foreach pairs

      nCandidates += candidates.size

      val box = Box(band)
      box.pr = pr
      box.id = pr.oid
      box.disks = candidates.toList

      box
    }
    val tBoxes = (clocktime - t_prime) - tPairs - tCenters - tCandidates

    stats.tPairs = tPairs
    stats.tCenters = tCenters
    stats.tCandidates = tCandidates

    stats.nPairs = nPairs
    stats.nCenters = nCenters
    stats.nCandidates = nCandidates
    stats.nBoxes = active_boxes.size

    active_boxes
  }

  def planeSweepingByPivot(points: List[STPoint], pivot: Point, time_instant: Int)(implicit S: Settings, G: GeometryFactory, stats: Stats): List[Box] = {

    // ordering by coordinate (it is first by x and then by y)...
    val (pointset, tSort) = timer {
      points.sortBy(_.getCoord)
    }
    stats.tSort = tSort

    // setting data structures to store candidates and boxes...
    var pairs: ListBuffer[(STPoint, STPoint)] = ListBuffer()
    var nPairs                                = 0
    var nCenters                              = 0
    var nCandidates                           = 0
    var tPairs                                = 0.0
    var tCenters                              = 0.0
    var tCandidates                           = 0.0

    // feeding bands with points inside 2-epsilon x 2-epsilon...
    val (bands, tBand) = timer {
      val tree = new STRtree()
      pointset.foreach { P =>
        tree.insert(P.point.getEnvelopeInternal, P)
      }
      pointset
        .map { pr: STPoint =>
          val band_for_pr: RTree[STPoint] = RTree[STPoint]()
          band_for_pr.pr = pr

          val env = new Envelope(pr.envelope)
          env.expandBy(S.epsilon)
          tree.query(env).asScala.map { _.asInstanceOf[STPoint] }.foreach { ps: STPoint =>
            band_for_pr.put(ps.envelope, ps)
          }

          band_for_pr
        }
        .filter(_.size() >= S.mu)
    }
    stats.tBand = tBand

    val t_prime      = clocktime
    val active_boxes = bands.map { band =>
      var candidates: ListBuffer[Disk] = new ListBuffer[Disk]
      val points                       = band.getAll[STPoint]
      val pr                           = band.pr
      val (band_pairs, tP)             = timer {
        points.filter { p =>
          p.X >= pr.X &&              // those at the right...
          p.oid != pr.oid &&          // prune duplicates...
          p.distance(pr) <= S.epsilon // getting pairs...
        }
      }
      pairs.appendAll(band_pairs.map(p => (pr, p)))
      nPairs += band_pairs.size
      tPairs += tP

      band_pairs.foreach { p =>
        val (band_centres, tC) = timer {
          val centre = computeCentres(pr, p)
            .map { centre => // pick centre closest to pivot...
              val dist = centre.distance(pivot)
              (dist, centre)
            }
            .minBy(_._1)
          List(centre._2) // gettings centres...
        }
        nCenters += band_centres.size
        tCenters += tC

        debug {
          band_centres.map { centre =>
            val wkt = centre.toText
            s"$wkt\t${S.epsilon}"
          } // .foreach{println}
        }

        band_centres.foreach { centre =>
          val t0       = clocktime
          val envelope = centre.getEnvelopeInternal
          envelope.expandBy(S.r)
          val hood = band.get[STPoint](envelope).filter { _.distanceToPoint(centre) <= S.r }

          if (hood.size >= S.mu) {
            // val c = G.createMultiPoint(hood.map(_.point).toArray).getCentroid
            // val candidate = Disk(c, hood.map(_.oid), time_instant, time_instant) // set with the default time instance...
            val candidate = Disk(centre, hood.map(_.oid), time_instant, time_instant) // set with the default time instance...
            candidates.append(candidate) // getting candidates...
          }
          tCandidates += (clocktime - t0) / 1e9
        } // foreach centres
      }   // foreach pairs

      nCandidates += candidates.size

      val box = Box(band)
      box.pr = pr
      box.id = pr.oid
      box.disks = candidates.toList

      box
    }
    val tBoxes = (clocktime - t_prime) - tPairs - tCenters - tCandidates

    stats.tPairs = tPairs
    stats.tCenters = tCenters
    stats.tCandidates = tCandidates

    stats.nPairs = nPairs
    stats.nCenters = nCenters
    stats.nCandidates = nCandidates
    stats.nBoxes = active_boxes.size

    active_boxes
  }

  def filterCandidates(boxes: List[Box])(implicit S: Settings, G: GeometryFactory, stats: Stats): List[Disk] = {

    @tailrec
    def itBoxes(boxes: List[Box], final_candidates: ListBuffer[Disk])(implicit S: Settings): List[Disk] = {
      boxes match {
        case box_i :: remain_boxes => {
          val candidates             = box_i.candidates
          val final_candidates_prime = itRemain(box_i, boxes, candidates, final_candidates, false)
          itBoxes(remain_boxes, final_candidates_prime)
        }
        case Nil => final_candidates.toList
      }
    }

    @tailrec
    def itRemain(box_i: Box, boxes: List[Box], candidates: List[Disk], final_candidates: ListBuffer[Disk], was_processed: Boolean)(implicit S: Settings): ListBuffer[Disk] = {
      boxes match {
        case box_j :: more_boxes => {
          if (math.abs(box_i.getCentroid.x - box_j.getCentroid.x) <= S.epsilon) { // boxes are close enough in X...
            if (box_i.intersects(box_j)) {                                        // boxes intersects...
              val final_candidates_prime = itCandidates(candidates, final_candidates)
              itRemain(box_i, List.empty[Box], candidates, final_candidates_prime, true)
            } else {
              itRemain(box_i, more_boxes, candidates, final_candidates, false)
            }
          } else {
            itRemain(box_i, List.empty[Box], candidates, final_candidates, false)
          }
        }
        case Nil =>
          if (was_processed)
            itCandidates(candidates, final_candidates)
          else
            final_candidates
      }
    }

    @tailrec
    def itCandidates(candidates: List[Disk], final_candidates: ListBuffer[Disk])(implicit S: Settings): ListBuffer[Disk] = {
      candidates match {
        case candidate :: remain_candidates => {
          val final_candidates_prime = insertDisk(final_candidates, candidate)
          itCandidates(remain_candidates, final_candidates_prime)
        }
        case Nil => final_candidates
      }
    }

    val maximals = itBoxes(boxes, new ListBuffer[Disk]())
    stats.nMaximals = maximals.size

    maximals.toList
  }

  /** Filter out candidate disks which are subsets. Implement Algorithm 2.
    *
    * @param boxes_prime
    *   set of active boxes (previous sorting).
    * @return
    *   set of maximal disks.
    */
  def filterCandidates2(boxes_prime: ArcheryRTree[Box])(implicit S: Settings, stats: Stats): List[Disk] = {
    // Sort boxes by left-bottom corner...
    val (boxes, tS) = timer {
      boxes_prime.values.toList.sortBy {
        _.left_bottom
      }
    }
    stats.tSort = tS

    debug {
      save("/tmp/edgesBCandidates.wkt") {
        boxes.flatMap { box =>
          val bid = box.id
          box.disks
            .sortBy(_.pidsText)
            .map { disk =>
              val pids     = disk.pidsText
              val disk_wkt = disk.getCircleWTK
              s"$disk_wkt\t$bid\t$pids\n"
            }
            .sorted
        }
      }
    }

    var C: ListBuffer[Disk] = ListBuffer()

    val (_, tI) = timer {
      if (boxes.nonEmpty) {
        boxes.size match {
          case 1 =>
            for (c <- boxes.head.disks) {
              C = insertDisk(C, c)
            }
          case _ =>
            for {
              j <- boxes.indices
            } yield {
              // println(s"Pruning disk in Box $j")
              for (c <- boxes(j).disks) {
                C = insertDisk(C, c)
              }
            }
        }
      }
    }

    C.toList
  }

  /** Prune list of candidate disks by iterating over each of them and removing subsets and duplicates.
    *
    * @param C
    *   list of current candidate disks.
    * @param c
    *   candidate disk.
    * @return
    *   list of maximal disks.
    */
  def insertDisk(C: ListBuffer[Disk], c: Disk)(implicit S: Settings): ListBuffer[Disk] = {
    var continue: Boolean = true
    for (d <- C if continue) {
      (c, d) match {
        case _ if { c.signature == d.signature } => // Manage especial case (hash conflict)...
          (c, d) match {
            case _ if c.isSubsetOf(d) => continue = false
            case _ if d.isSubsetOf(c) => C -= d
            case _                    => /* Just continue */
          }
        case _ if { c & d && c.distance(d) <= S.epsilon } => if (c.isSubsetOf(d)) continue = false
        case _ if { d & c && d.distance(c) <= S.epsilon } => if (d.isSubsetOf(c)) C -= d
        case _                                            => /* Just continue... */
      }
    }

    if (!continue) C else C += c
  }

  /** Run the PSI algorithm.
    * @param points
    *   list of points.
    */
  def run(points: List[STPoint], time_instant: Int = 0)(implicit S: Settings, G: GeometryFactory): (List[Disk], Stats) = {

    // For debugging purposes...
    implicit val stats: Stats = Stats()
    stats.nPoints = points.size

    // Call plane sweeping technique algorithm...
    val boxes = PSI.planeSweeping(points, time_instant)
    debug {
      boxes.foreach { box =>
        // println(s"${box.wkt}\t${box.id}\t${box.disks}")
      }
    }

    // Call filter candidates algorithm...
    val (maximals, tF) = timer {
      PSI.filterCandidates(boxes)
    }
    stats.tFilter = tF

    (maximals, stats)
  }

  def runByPivot(points: List[STPoint], pivot: Point, time_instant: Int = 0)(implicit S: Settings, G: GeometryFactory): (List[Disk], Stats) = {

    // For debugging purposes...
    implicit val stats: Stats = Stats()
    stats.nPoints = points.size

    // Call plane sweeping technique algorithm...
    val boxes = PSI.planeSweepingByPivot(points, pivot, time_instant)
    debug {
      boxes.foreach { box =>
        // println(s"${box.wkt}\t${box.id}\t${box.disks}")
      }
    }

    // Call filter candidates algorithm...
    val (maximals, tF) = timer {
      PSI.filterCandidates(boxes)
    }
    stats.tFilter = tF

    (maximals, stats)
  }

  def main(args: Array[String]): Unit = {
    implicit var S: Settings                 = Setup.getSettings(args) // Initializing settings...
    implicit val geofactory: GeometryFactory = new GeometryFactory(new PrecisionModel(S.scale))

    val points = readPoints(S.dataset)
    log(s"START")

    val (maximals, stats) = PSI.run(points)
    stats.printPSI()

    debug {
      save("/tmp/edgesPointsPSI.wkt") { points.map { _.wkt + "\n" } }
      save("/tmp/edgesMaximalsPSI.wkt") { maximals.map { _.wkt + "\n" } }
      save("/tmp/edgesMaximalsPSI_prime.wkt") { maximals.map { _.getCircleWTK + "\n" } }
    }

    log(s"END")
  }
}
