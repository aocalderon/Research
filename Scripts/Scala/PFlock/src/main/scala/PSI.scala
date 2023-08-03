package edu.ucr.dblab.pflock

import org.slf4j.{Logger, LoggerFactory}

import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree

import archery.{RTree => ArcheryRTree, Entry, Box => ArcheryBox}

import scala.collection.JavaConverters._
import edu.ucr.dblab.pflock.Utils._

object PSI {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  /**
    * Simple wrapper for the JTS STRtree implementation.
    **/
  case class RTree[T]() extends STRtree(){
    var minx: Double = Double.MaxValue
    var miny: Double = Double.MaxValue
    var maxx: Double = Double.MinValue
    var maxy: Double = Double.MinValue

    /**
      * Return the most left bottom corner of all the points stored in the RTree.
      **/
    def left_bottom: Coordinate = new Coordinate(minx, miny)

    /**
      * Return the most right top corner of all the points stored in the RTree.
      **/
    def   right_top: Coordinate = new Coordinate(maxx, maxy)

    /**
      * Return the extend of the points stored in the RTree.
      **/
    def    envelope: Envelope   = new Envelope(left_bottom, right_top)

    /**
      * Store the envelope attached to the element in the RTree.
      * @param envelope envelope of the element.
      * @param element the element to be stored.
      **/
    def put[T](envelope: Envelope, element: T)(implicit S: Settings): Unit = {
      if(envelope.getMinX < minx) minx = envelope.getMinX - S.tolerance // to fix precision issues...
      if(envelope.getMinY < miny) miny = envelope.getMinY - S.tolerance
      if(envelope.getMaxX > maxx) maxx = envelope.getMaxX + S.tolerance
      if(envelope.getMaxY > maxy) maxy = envelope.getMaxY + S.tolerance

      super.insert(envelope, element)
    }

    /**
      * Return a list of elements enclosed by an envelope.
      * @param envelope the envelope.
      * @return list a list of elements.
      **/
    def get[T](envelope: Envelope): List[T] = {
      super.query(envelope).asScala.map{_.asInstanceOf[T]}.toList
    }

    /**
      * Check if the envelope already exists in the RTree.
      * @param envelope the envelope
      * @return boolean true if exists, false otherwise.
      **/
    def exists[T](envelope: Envelope): Boolean = {
      this.get(envelope).exists { element: T =>
          if(element.isInstanceOf[Geometry]){
            element.asInstanceOf[Geometry].getEnvelopeInternal.compareTo(envelope) == 0
          } else {
            false
          }
      }
    }

    /**
      * Return all the elements stored in the RTree.
      * @return list a list of elements.
      **/
    def getAll[T]: List[T] = super.query(this.envelope).asScala.map(_.asInstanceOf[T]).toList
  }

  /**
    * Stores the active boxes for candidates.
    * @constructor create a box from an envelope and the point it belongs
    * @param envelope the envelope defining the box.
    * @param point    the point this box belongs to.
    **/
  case class Box(hood: RTree[STPoint]) extends Envelope(hood.envelope){
    val points: List[STPoint] = hood.getAll[STPoint]
    val envelope: Envelope = hood.envelope
    val left_bottom: Coordinate = new Coordinate(envelope.getMinX, envelope.getMinY)

    def boundingBox(implicit S: Settings): ArcheryBox = {
      ArcheryBox(getMinX.toFloat, getMinY.toFloat, getMaxX.toFloat, getMaxY.toFloat)
    }

    def archeryEntry(implicit S: Settings): archery.Entry[Box] = archery.Entry(this.boundingBox, this)

    def wkt(implicit G: GeometryFactory): String = G.toGeometry(this).toText
  }

  /**
    * Return an archery bounding Box from a JTS point.
    * @param point the JTS point.
    * @return box the archery bounding Box.
    */
  private def JTSPointBBox(point: Point)(implicit S: Settings): archery.Box = {
    val x = point.getX.toFloat
    val y = point.getY.toFloat
    val r = S.r.toFloat
    archery.Box(x - r, y - r, x + r, y + r)
  }

  /**
    * Compute the envelope of all the points in the RTree.
    * @param tree the RTree.
    * @return envelope the Envelope.
    **/
  def getEnvelope(tree: ArcheryRTree[STPoint]): Envelope = {

    // the recursive function...
    @annotation.tailrec
    def get_envelope(points: List[STPoint], envelope: Envelope): Envelope = {
      points match {
        case Nil => envelope
        case headPoint :: tailPoints => {
          val x_min = if(envelope.getMinX < headPoint.X) envelope.getMinX else headPoint.X
          val y_min = if(envelope.getMinY < headPoint.Y) envelope.getMinY else headPoint.Y
          val x_max = if(envelope.getMaxX > headPoint.X) envelope.getMaxX else headPoint.X
          val y_max = if(envelope.getMaxY > headPoint.Y) envelope.getMaxY else headPoint.Y
          get_envelope(tailPoints, new Envelope(x_min, x_max, y_min, y_max))
        }
      }
    }

    // the main call...
    get_envelope(tree.values.toList, new Envelope())
  }

  /**
    * Find candidates disks and active boxes with plane sweeping technique.
    * @param points the set of points.
    * @return list of candidates and list of boxes.
    **/
  def planeSweeping(points: List[STPoint])
    (implicit S: Settings, G: GeometryFactory): (RTree[Disk], ArcheryRTree[Box]) = {
    
    // ordering by coordinate (it is first by x and then by y)...
    val pointset: List[STPoint] = points.sortBy(_.getCoord)

    debug{
      println("Pointset...")
      pointset
        .zipWithIndex
        .map{ case(point, order) => s"${point.wkt}\t${order}" }
        .foreach{println}
    }

    // setting data structures to store candidates and boxes...
    val candidates: RTree[Disk] = RTree[Disk]
    var boxes: ArcheryRTree[Box] = ArcheryRTree[Box]()

    // feeding candidates and active boxes...
    pointset.foreach{ pr: STPoint =>

      // feeding band with points inside 2-epsilon x 2-epsilon...
      val band_for_pr: RTree[STPoint] = RTree[STPoint]
      pointset.filter{ ps: STPoint =>
        val q = math.abs(ps.X - pr.X) <= S.epsilon && math.abs(ps.Y - pr.Y) <= S.epsilon
        println(s"Band for ${pr.oid} and ${ps.oid} = $q")
        q
      }.foreach{ ps: STPoint =>
        band_for_pr.put(ps.envelope, ps)
      }

      // finding pairs of points, centers, candidates and boxes...
      val band_pairs = band_for_pr.getAll[STPoint]
        .filter{ p =>
          p.X >= pr.X &&
          p.oid != pr.oid &&
          p.distance(pr) <= S.epsilon // getting pairs...
        }.toList

      debug{
        println("Band_pairs...")
        band_pairs.foreach{println}
      }

      band_pairs.foreach{ p =>
        val band_centres = computeCentres(pr, p) // gettings centres...

        debug{
          band_centres.map{_.buffer(S.r, 25).toText}.foreach{println}
        }

        band_centres.foreach{ centre =>
          val envelope = centre.getEnvelopeInternal
          envelope.expandBy(S.r)
          val hood = band_for_pr
            .get[STPoint](envelope)
            .filter{ _.distanceToPoint(centre) <= S.r }
            .map(_.oid)
            .toList

          if(hood.size >= S.mu){
            val candidate = Disk(centre, hood)
            candidates.put(candidate.envelope, candidate) // getting candidates...

            val active_box = Box(band_for_pr)
            if(!boxes.values.exists(_ == active_box)){
              println("inserting BBox...")
              boxes = boxes.insert(active_box.archeryEntry) // getting boxes...
            }
          }
        }
      }

      debug{
        println(s"Boxes for ${pr.oid} so far...")
        boxes.values.map{_.wkt}.foreach{println}
      }
    } // foreach pointset

    (candidates, boxes)
  }

  private def insertDisk(candidates: ArcheryRTree[Disk], candidate: Disk)
    (implicit S: Settings): ArcheryRTree[Disk] = {

    @annotation.tailrec
    def insert_disk(candidates: ArcheryRTree[Disk], candidate: Disk, hood: List[Disk])
        : ArcheryRTree[Disk] = {

      hood match {
        case maximal :: hood_prime => {
          (maximal, candidate) match {
            case _ if{ (maximal & candidate) && candidate.isSubsetOf(maximal) } => {
              candidates
            }
            case _ if{ (candidate & maximal) && maximal.isSubsetOf(candidate) } => {
              insert_disk(candidates.remove(maximal.archeryEntry), candidate, hood_prime)
            }
            case _ => {
              insert_disk(candidates, candidate, hood_prime)
            }
          }
        }
        case Nil => candidates.insert(candidate.archeryEntry)
      }
    }

    val hood: List[Disk] = candidates
      .remove(candidate.pointEntry)
      .search(candidate.bbox(S.r.toFloat))
      .map(_.value)
      .filter(_.distance(candidate) < S.epsilon)
      .toList

    insert_disk(candidates, candidate, hood)
  }

  private def insertBox(candidates: ArcheryRTree[Disk], box: Box)(implicit S: Settings): ArcheryRTree[Disk] = {

    @annotation.tailrec
    def insert_box(disks: List[Disk], candidates: ArcheryRTree[Disk]): ArcheryRTree[Disk] = {
      disks match {
        case candidate :: disks_prime => {
          val candidates_prime = insertDisk(candidates.remove(candidate.pointEntry), candidate)
          insert_box(disks_prime, candidates_prime)
        }
        case Nil => candidates
      }
    }

    val disks: List[Disk] = candidates.search(box.boundingBox).map(_.value).toList

    insert_box(disks, candidates)
  }

  /**
    * Filter out candidate disks which are subsets. Implement Algorithm 2.
    * @param candidates current set of candidates disks.
    * @param boxes set of active boxes.
    * @return set of maximal disks.
    */
  def filterCandidates(candidates: RTree[Disk], boxes: ArcheryRTree[Box])
    (implicit S: Settings): List[Disk] = {

    @annotation.tailrec
    def filter_candidates(sorted: List[Box], lookup: ArcheryRTree[Box],
      candidates: ArcheryRTree[Disk]): ArcheryRTree[Disk] = {

      sorted match {
        case current_box :: sorted_prime => {
          val candidates_prime =
            if(lookup.search(current_box.boundingBox).size > 0){
              insertBox(candidates, current_box)
            } else {
              candidates
            }
          // recurse...
          if(!sorted_prime.isEmpty){
          val next_box = sorted_prime.head
          val lookup_prime = lookup.remove(next_box.archeryEntry) // remove next box from lookup

            filter_candidates(sorted_prime, lookup_prime, candidates_prime)
          } else {
            candidates
          }
        }
        case Nil => candidates
      }
    }

    // C is the initial set of candidates...
    val C: List[Entry[Disk]] = candidates.getAll[Disk].map(_.pointEntry)
    val candidate_disks: ArcheryRTree[Disk] = ArcheryRTree[Disk](C: _*)
    // Sort boxes by left-bottom corner...
    val sorted_boxes: List[Box] = boxes.values.toList.sortBy{ _.left_bottom }

    // call Algorithm 2
    val r = filter_candidates(sorted_boxes, boxes, candidate_disks)

    r.entries.map{_.value}.toList

  }

  /**
    * Run the PSI algorithm.
    * @param list list of points.
    **/
  def run(points: List[STPoint])(implicit S: Settings, G: GeometryFactory, L: Logger): List[Disk] = {

    val (candidates, boxes) = PSI.planeSweeping(points)
    debug{
      save("/tmp/edgesC.wkt"){ candidates.getAll[Disk].map{ _.wkt + "\n" } }
      save("/tmp/edgesC_prime.wkt"){ candidates.getAll[Disk].map{ _.getCircleWTK + "\n" } }
      save("/tmp/edgesB.wkt"){ boxes.values.map{ _.wkt + "\n" }.toList }
    }

    PSI.filterCandidates(candidates, boxes)
    
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit var S = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = params.method(),
      capacity = params.capacity(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      appId = System.nanoTime().toString()
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(S.scale))

    val points = readPoints(S.dataset)
    log(s"START")

    val maximals = PSI.run(points)

    debug{
      save("/tmp/edgesM.wkt"){ maximals.map{ _.wkt + "\n" } }
    }


    log(s"END")
  }
}
