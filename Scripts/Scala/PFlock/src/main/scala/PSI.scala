package edu.ucr.dblab.pflock

import archery.{RTree => ArcheryRTree}
import edu.ucr.dblab.pflock.PSI_Utils._
import edu.ucr.dblab.pflock.Utils._
import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object PSI {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")


  /**
    * Find candidates disks and active boxes with plane sweeping technique.
    * @param points the set of points.
    * @return list of candidates and list of boxes.
    **/
  def planeSweeping(points: List[STPoint])
    (implicit S: Settings, G: GeometryFactory, stats: Stats): (RTree[Disk], ArcheryRTree[Box]) = {

    // ordering by coordinate (it is first by x and then by y)...
    val pointset: List[STPoint] = points.sortBy(_.getCoord)

    debug{
      save("/tmp/edgesPointsPSI.wkt"){
        pointset
          .zipWithIndex
          .map{ case(point, order) => s"${point.wkt}\t$order\n" }
      }
    }

    // setting data structures to store candidates and boxes...
    val candidates: RTree[Disk] = RTree[Disk]()
    var boxes: ArcheryRTree[Box] = ArcheryRTree[Box]()
    var pairs: ListBuffer[(STPoint, STPoint)] = ListBuffer()
    var nPairs = 0
    var nCenters = 0
    var tBand = 0.0
    var tPairs = 0.0
    var tCenters = 0.0
    var tCandidates = 0.0
    var tBoxes = 0.0

    // feeding candidates and active boxes...
    pointset.foreach{ pr: STPoint =>
      // feeding band with points inside 2-epsilon x 2-epsilon...
      val band_for_pr: RTree[STPoint] = RTree[STPoint]()
      val (_, tB) = timer {
        pointset.filter { ps: STPoint =>
          math.abs(ps.X - pr.X) <= S.epsilon && math.abs(ps.Y - pr.Y) <= S.epsilon
        }.foreach { ps: STPoint =>
          band_for_pr.put(ps.envelope, ps)
        }
      }
      tBand += tB

      // finding pairs of points, centers, candidates and boxes...
      val (band_pairs, tP) = timer{
        band_for_pr.getAll[STPoint]
          .filter{ p =>
            p.X >= pr.X &&  // those at the right...
              p.oid != pr.oid && // prune duplicates...
              p.distance(pr) <= S.epsilon // getting pairs...
          }
      }
      pairs.appendAll(band_pairs.map(p => (pr, p)))
      nPairs += band_pairs.size
      tPairs += tP

      band_pairs.foreach{ p =>
        val (band_centres, tC) = timer{
          computeCentres(pr, p) // gettings centres...
        }
        nCenters += band_centres.size
        tCenters += tC

        band_centres.foreach{ centre =>
          val t0 = clocktime
          val envelope = centre.getEnvelopeInternal
          envelope.expandBy(S.r)
          val hood = band_for_pr
            .get[STPoint](envelope)
            .filter{ _.distanceToPoint(centre) <= S.r }

          val t1 = if(hood.size >= S.mu){
            val candidate = Disk(centre, hood.map(_.oid))
            candidates.put(candidate.envelope, candidate) // getting candidates...
            val t1 = clocktime

            val active_box = Box(band_for_pr, pr.oid)

            debug{
                println(active_box.wkt)
            }

            val active_box_hood = boxes.searchIntersection(active_box.boundingBox)

            def coveredBy: Boolean = active_box_hood.exists(b => b.value.covers(active_box))
            def    covers: Boolean = active_box_hood.exists(b => active_box.covers(b.value))
            if(coveredBy){
              // do nothing...
            } else if(covers){
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
    stats.tBoxes = tBoxes / 1e9

    debug{
      save("/tmp/edgesPairsPSI.wkt"){
        pairs.toList.map{ case(pr, p) =>
          val coords = Array(pr.getCoord, p.getCoord)
          G.createLineString(coords).toText + "\n"
        }
      }
    }

    (candidates, boxes)
  }

  def planeSweeping2(points: List[STPoint])
                   (implicit S: Settings, G: GeometryFactory, stats: Stats): (RTree[Disk], ArcheryRTree[Box]) = {

    // ordering by coordinate (it is first by x and then by y)...
    val pointset: List[STPoint] = points.sortBy(_.getCoord)

    debug {
      save("/tmp/edgesPointsPSI.wkt") {
        pointset
          .zipWithIndex
          .map { case (point, order) => s"${point.wkt}\t$order\n" }
      }
    }

    // setting data structures to store candidates and boxes...
    val candidates: RTree[Disk] = RTree[Disk]()
    var boxes: ArcheryRTree[Box] = ArcheryRTree[Box]()
    var pairs: ListBuffer[(STPoint, STPoint)] = ListBuffer()
    var nPairs = 0
    var nCenters = 0
    var tPairs = 0.0
    var tCenters = 0.0
    var tCandidates = 0.0
    var tBoxes = 0.0

    // feeding bands with points inside 2-epsilon x 2-epsilon...
    val (bands, tBand) = timer {
      pointset.map { pr: STPoint =>
        val band_for_pr: RTree[STPoint] = RTree[STPoint]()
        band_for_pr.pr = pr

        pointset.filter { ps: STPoint =>
          math.abs(ps.X - pr.X) <= S.epsilon && math.abs(ps.Y - pr.Y) <= S.epsilon
        }.foreach { ps: STPoint =>
          band_for_pr.put(ps.envelope, ps)
        }

        band_for_pr
      }.filter(_.size() >= S.mu)
    }

    debug{
      bands.zipWithIndex.foreach{ case(band, i) =>
        println(G.toGeometry(band.envelope).toText + s"\t$i")
      }
    }

    bands.foreach{ band =>
      var candidates: ListBuffer[Disk] = new ListBuffer[Disk]
      val points = band.getAll[STPoint]
      val pr = band.pr
      val (band_pairs, tP) = timer {
        points.filter { p =>
          p.X >= pr.X && // those at the right...
            p.oid != pr.oid && // prune duplicates...
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
          val t0 = clocktime
          val envelope = centre.getEnvelopeInternal
          envelope.expandBy(S.r)
          val hood = band.get[STPoint](envelope).filter { _.distanceToPoint(centre) <= S.r }

          if (hood.size >= S.mu) {
            val candidate = Disk(centre, hood.map(_.oid))
            candidates.append(candidate) // getting candidates...
          }
          tCandidates += (clocktime - t0)
        } // foreach centres
      } // foreach pairs

      candidates.toList.foreach(println)
    }

    // feeding candidates and active boxes...
    pointset.foreach { pr: STPoint =>
      // feeding band with points inside 2-epsilon x 2-epsilon...
      val band_for_pr: RTree[STPoint] = RTree[STPoint]()
      val (_, tB) = timer {
        pointset.filter { ps: STPoint =>
          math.abs(ps.X - pr.X) <= S.epsilon && math.abs(ps.Y - pr.Y) <= S.epsilon
        }.foreach { ps: STPoint =>
          band_for_pr.put(ps.envelope, ps)
        }
      }

      // finding pairs of points, centers, candidates and boxes...
      val (band_pairs, tP) = timer {
        band_for_pr.getAll[STPoint]
          .filter { p =>
            p.X >= pr.X && // those at the right...
              p.oid != pr.oid && // prune duplicates...
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
          val t0 = clocktime
          val envelope = centre.getEnvelopeInternal
          envelope.expandBy(S.r)
          val hood = band_for_pr
            .get[STPoint](envelope)
            .filter {
              _.distanceToPoint(centre) <= S.r
            }

          val t1 = if (hood.size >= S.mu) {
            val candidate = Disk(centre, hood.map(_.oid))
            candidates.put(candidate.envelope, candidate) // getting candidates...
            val t1 = clocktime

            val active_box = Box(band_for_pr, pr.oid)

            val active_box_hood = boxes.searchIntersection(active_box.boundingBox)

            def coveredBy: Boolean = active_box_hood.exists(b => b.value.covers(active_box))

            def covers: Boolean = active_box_hood.exists(b => active_box.covers(b.value))

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
      } // foreach pairs
    } // foreach pointset
    stats.nPairs = nPairs
    stats.nCenters = nCenters
    stats.nCandidates = candidates.size
    stats.nBoxes = boxes.size
    stats.tBand = tBand
    stats.tPairs = tPairs
    stats.tCenters = tCenters
    stats.tCandidates = tCandidates / 1e9
    stats.tBoxes = tBoxes / 1e9

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

  /**
    * Filter out candidate disks which are subsets. Implement Algorithm 2.
    *
    * @param boxes_prime  set of active boxes (previous sorting).
    * @return set of maximal disks.
    */
  def filterCandidates(boxes_prime: ArcheryRTree[Box])(implicit S: Settings, stats: Stats): List[Disk] = {
    // Sort boxes by left-bottom corner...
    val (boxes, tS) = timer{
      boxes_prime.values.toList.sortBy {
        _.left_bottom
      }
    }
    stats.tSort = tS

    if (S.debug) {
      save("/tmp/edgesBCandidates.wkt") {
        boxes.flatMap { box =>
          val bid = box.id
          box.disks.sortBy(_.pidsText).map { disk =>
            val pids = disk.pidsText
            val disk_wkt = disk.getCircleWTK
            s"$disk_wkt\t$bid\t$pids\n"
          }.sorted
        }
      }
    }

    var C: ListBuffer[Disk] = ListBuffer()

    val (_, tI) = timer{
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
              //println(s"Pruning disk in Box $j")
              for (c <- boxes(j).disks) {
                C = insertDisk(C, c)
              }
            }
        }
      }
    }
    stats.tInsert = tI

    C.toList
  }

  /**
    * Prune list of candidate disks by iterating over their each of them and removing subsets and duplicates.
    *
    * @param C  list of current candidate disks.
    * @param c  candidate disk.
    * @return list of maximal disks.
    */
  def insertDisk( C: ListBuffer[Disk], c: Disk)(implicit S: Settings): ListBuffer[Disk] = {
    var continue: Boolean = true
    for( d <- C if continue){

      debug{
        if (c.pidsText == "291 316 342 441 448 546 666 737" && d.pidsText == "291 316 342 441 448 546 737") {
          println(s"c: ${c.pidsText}")
          println(s"d: ${d.pidsText}")
          println(s"c: ${c.toBinarySignature}")
          println(s"d: ${d.toBinarySignature}")
        }
      }

      (d, c) match {
        case _ if {
          c.signature == d.signature
        } =>
          if (c.isSubsetOf(d)) {
            continue = false
          } else if (d.isSubsetOf(c)) {
            C -= d
          }
        case _ if {
          c & d && c.distance(d) <= S.epsilon
        } =>
          if (c.isSubsetOf(d)) {
            continue = false
          }
        case _ if {
          d & c && d.distance(c) <= S.epsilon
        } =>
          if (d.isSubsetOf(c)) {
            C -= d
          }
        case _ => /* Just continue... */
      }
    }

    if( !continue ) C else C += c 
  }

  /**
    * Run the PSI algorithm.
    * @param points list of points.
    **/
  def run(points: List[STPoint])(implicit S: Settings, G: GeometryFactory): (List[Disk], Stats) = {

    // For debugging purposes...
    implicit val stats: Stats = Stats()
    stats.nPoints = points.size

    // Call plane sweeping technique algorithm...

    val ((candidates, boxes), tPS) = timer{
      PSI.planeSweeping2(points)
    }
    stats.tPS = tPS

    val candidates_prime = new STRtree()
    candidates.getAll[Disk]
      .groupBy{ candidate =>
        candidate.pidsText
      }
      .map{ case(_, disks) =>
        disks.head
      }
      .foreach{ candidate =>
        candidates_prime.insert(candidate.getExpandEnvelope(S.r), candidate)
      }

    // Feed each box with the disks it contains...
    boxes.values.zipWithIndex.foreach{ case(box, id) =>
      box.id = id
      box.disks = candidates_prime.query(box).asScala
        .map{_.asInstanceOf[Disk]}
        .filter{_.center.distance(G.toGeometry(box)) <= S.r} // the disk intersects the box...
        .toList
      box.pidsSet = box.disks.map(_.pids).toSet
    }

    debug {
      save("/tmp/edgesBoxesPSI.wkt") {
        boxes.values.map { box =>
          s"${box.wkt}\t${box.id}\t${box.pr}\t${box.diagonal}\t${box.pidsSet.map(_.mkString(" ")).mkString("; ")}\n"
        }.toList
      }
      save(s"/tmp/boxes_${S.dataset_name}.tsv") {
        boxes.values.flatMap { box =>
          box.disks.map{ disk =>
            s"${box.id}\t${disk.pidsText}\n"
          }
        }.toList
      }
      save(s"/tmp/pids.tsv") {
        boxes.values.flatMap { box =>
          val id = box.id
          box.pidsSet.flatten.toList.map { pid =>
            s"$pid\t$id\n"
          }
        }.toList
      }
      save(s"/tmp/samples.tsv") {
        val pids = boxes.values.flatMap { box =>
          val id = box.id
          box.pidsSet.flatten.toList.map{ pid =>
            (pid, id)
          }.toSet
        }.toList

        for{
          b <- pids
          p <- points if b._1 == p.oid
        } yield {
          s"${p.oid}\t${p.point.getX}\t${p.point.getY}\t0\t${b._2}\n"
        }
      }
    }

    // Call filter candidates algorithm...
    val (maximals, tFC) = timer{
      PSI.filterCandidates(boxes)
    }
    stats.nMaximals = maximals.size
    stats.tFC = tFC

    (maximals, stats)
  }

  def main(args: Array[String]): Unit = {
    implicit val params: BFEParams = new BFEParams(args)

    implicit val S: Settings = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = params.method(),
      capacity = params.capacity(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      tester = params.tester(),
      appId = System.nanoTime().toString
    )
    implicit val geofactory: GeometryFactory = new GeometryFactory(new PrecisionModel(S.scale))

    val points = readPoints(S.dataset)
    log(s"START")

    val (maximals, stats) = PSI.run(points)

    if(S.debug){
      save("/tmp/edgesPointsPSI.wkt"){ points.map{ _.wkt + "\n" } }
      save("/tmp/edgesMaximalsPSI.wkt"){ maximals.map{ _.wkt + "\n" } }
      save("/tmp/edgesMaximalsPSI_prime.wkt"){ maximals.map{ _.getCircleWTK + "\n" } }

      stats.printPSI()
    }

    log(s"END")
  }
}
