package edu.ucr.dblab.pflock

import org.slf4j.{Logger, LoggerFactory}

import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree

import scala.collection.JavaConverters._
import edu.ucr.dblab.pflock.Utils._

object PSI {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  /** Simple wrapper for the JTS STRtree implementation.
    **/
  case class RTree[T]() extends STRtree(){
    var minx: Double = Double.MaxValue
    var miny: Double = Double.MaxValue
    var maxx: Double = Double.MinValue
    var maxy: Double = Double.MinValue

    def left_bottom: Coordinate = new Coordinate(minx, miny)
    def   right_top: Coordinate = new Coordinate(maxx, maxy)
    def    envelope: Envelope   = new Envelope(left_bottom, right_top)

    def put[T](envelope: Envelope, element: T): Unit = {
      if(envelope.getMinX < minx) minx = envelope.getMinX
      if(envelope.getMinY < miny) miny = envelope.getMinY
      if(envelope.getMaxX > maxx) maxx = envelope.getMaxX
      if(envelope.getMaxY > maxy) maxy = envelope.getMaxY

      super.insert(envelope, element)
    }

    def get[T](envelope: Envelope): List[T] = {
      super.query(envelope).asScala.map{_.asInstanceOf[T]}.toList
    }

    def getAll[T]: List[T] = super.query(this.envelope).asScala.map(_.asInstanceOf[T]).toList
  }

  /** Stores the active boxes for candidates.
    * @constructor create a box from an envelope and the point it belongs
    * @param envelope the envelope defining the box.
    * @param point    the point this box belongs to.
    **/
  case class Box(hood: RTree[STPoint], point: STPoint) extends Envelope(hood.envelope){
    val points: List[STPoint] = hood.getAll[STPoint]
    val left_bottom: Coordinate = hood.left_bottom

    def wkt(implicit G: GeometryFactory): String = G.toGeometry(this).toText
  }

  /** Find candidates disks and active boxes with plane sweeping technique.
    * @param points the set of points.
    * @return list of candidates and list of boxes.
    **/
  def planeSweeping(points: List[STPoint])
    (implicit S: Settings, G: GeometryFactory): (RTree[Disk], RTree[Box]) = {
    
    // ordering by coordinate (it is first by x and then by y)...
    val pointset: List[STPoint] = points.sortBy(_.getCoord)

    // setting data structures to store candidates and boxes...
    val candidates: RTree[Disk] = RTree[Disk]
    val boxes: RTree[Box] = RTree[Box]

    // feeding candidates and active boxes...
    pointset.foreach{ pr: STPoint =>

      // feeding band with points inside 2-epsilon x 2-epsilon...
      val band: RTree[STPoint] = RTree[STPoint] 
      pointset.filter{ ps: STPoint =>
        math.abs(ps.X - pr.X) <= S.epsilon &&
        math.abs(ps.Y - pr.Y) <= S.epsilon
      }.foreach{ ps: STPoint =>
        band.put(ps.envelope, ps)
      }

      // finding pairs of points, centers, candidates and boxes...
      band.getAll[STPoint]
        .filter{ p =>
          p.X >= pr.X &&
          p.oid < pr.oid &&
          p.distance(pr) <= S.epsilon // getting pairs...
        }
        .foreach{ p =>
          val centres = computeCentres(pr, p) // getting centres...
          centres.foreach{ centre =>
            val envelope = new Envelope(centre.getEnvelopeInternal)
            envelope.expandBy(S.r)
            val hood = band
              .get[STPoint](envelope)
              .filter{ _.distanceToPoint(centre) <= S.r }
              .map(_.oid)

            if(hood.size >= S.mu){
              val candidate = Disk(centre, hood)
              candidates.put(candidate.envelope, candidate) // getting candidates...

              val active_box = Box(band, pr)
              boxes.put(active_box, active_box) // getting boxes...
            }
          }
        }
    }

    (candidates, boxes)
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit var settings = Settings(
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
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    val points = readPoints(settings.dataset)
    log(s"START")

    val (candidates, boxes) = PSI.planeSweeping(points)

    debug{
      save("/tmp/edgesC.wkt"){ candidates.getAll[Disk].map{ _.wkt + "\n" } }
      save("/tmp/edgesC_prime.wkt"){ candidates.getAll[Disk].map{ _.getCircleWTK + "\n" } }
      save("/tmp/edgesB.wkt"){ boxes.getAll[Box].map{ _.wkt + "\n" } }
    }

    log(s"END")
  }
}
