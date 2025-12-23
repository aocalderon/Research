package puj

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree

import archery.{RTree => ArcheryRTree, Box => ArcheryBox}

import scala.collection.JavaConverters._

import puj._
import puj.Utils._

import streaminer.SpookyHash

object PSI_Utils extends Logging {
  /**
    * Simple wrapper for the JTS STRtree implementation.
    * */
  case class RTree[T]() extends STRtree() {
    var minx: Double = Double.MaxValue
    var miny: Double = Double.MaxValue
    var maxx: Double = Double.MinValue
    var maxy: Double = Double.MinValue
    var pr:  STPoint = null

    /**
      * Return the most left bottom corner of all the points stored in the RTree.
      * */
    def left_bottom: Coordinate = new Coordinate(minx, miny)

    /**
      * Return the most right top corner of all the points stored in the RTree.
      * */
    def right_top: Coordinate = new Coordinate(maxx, maxy)

    /**
      * Return the extend of the points stored in the RTree.
      * */
    def envelope: Envelope = new Envelope(left_bottom, right_top)

    /**
      * Store the envelope attached to the element in the RTree.
      *
      * @param envelope envelope of the element.
      * @param element  the element to be stored.
      * */
    def put[T](envelope: Envelope, element: T)(implicit S: Settings): Unit = {
      if (envelope.getMinX < minx) minx = envelope.getMinX - S.tolerance // to fix precision issues...
      if (envelope.getMinY < miny) miny = envelope.getMinY - S.tolerance
      if (envelope.getMaxX > maxx) maxx = envelope.getMaxX + S.tolerance
      if (envelope.getMaxY > maxy) maxy = envelope.getMaxY + S.tolerance
      super.insert(envelope, element)
    }

    /**
      * Return a list of elements enclosed by an envelope.
      *
      * @param envelope the envelope.
      * @return list a list of elements.
      * */
    def get[T](envelope: Envelope): List[T] = {
      super.query(envelope).asScala.map {
        _.asInstanceOf[T]
      }.toList
    }

    /**
      * Check if the envelope already exists in the RTree.
      *
      * @param envelope the envelope
      * @return boolean true if exists, false otherwise.
      * */
    def exists[T](envelope: Envelope): Boolean = {
      this.get(envelope).exists { element: T =>
        if (element.isInstanceOf[Geometry]) {
          element.asInstanceOf[Geometry].getEnvelopeInternal.compareTo(envelope) == 0
        } else {
          false
        }
      }
    }

    /**
      * Return all the elements stored in the RTree.
      *
      * @return list a list of elements.
      * */
    def getAll[T]: List[T] = super.query(this.envelope).asScala.map(_.asInstanceOf[T]).toList

  }

  /**
    * Stores the active boxes for candidates.
    *
    * @constructor create a box from an envelope and the point it belongs
    * @param hood an RTree with the points and envelope defining the box.
    * */
  case class Box(hood: RTree[STPoint]) extends Envelope(hood.envelope) {
    val points: List[STPoint] = hood.getAll[STPoint]
    val envelope: Envelope = hood.envelope
    val left_bottom: Coordinate = hood.left_bottom
    var disks: List[Disk] = _
    var pidsSet: Set[List[Int]] = _
    var id: Int = -1
    var pr: STPoint = NullPoint

    def getCentroid: Coordinate = envelope.centre()

    def candidates: List[Disk] = disks

    def diagonal(implicit G: GeometryFactory): String = {
      val right_top: Coordinate = new Coordinate(envelope.getMaxX, envelope.getMaxY)
      G.createLineString(Array(left_bottom, right_top)).toString
    }

    def boundingBox(implicit S: Settings): ArcheryBox = {
      ArcheryBox(getMinX.toFloat, getMinY.toFloat, getMaxX.toFloat, getMaxY.toFloat)
    }

    def archeryEntry(implicit S: Settings): archery.Entry[Box] = archery.Entry(this.boundingBox, this)
    
    def wkt(implicit G: GeometryFactory): String = G.toGeometry(this).toText

  }

  /**
    * Compute the envelope of all the points in the RTree.
    *
    * @param tree the RTree.
    * @return envelope the Envelope.
    * */
  def getEnvelope(tree: ArcheryRTree[STPoint]): Envelope = {

    // the recursive function...
    @annotation.tailrec
    def get_envelope(points: List[STPoint], envelope: Envelope): Envelope = {
      points match {
        case Nil => envelope
        case headPoint :: tailPoints => {
          val x_min = if (envelope.getMinX < headPoint.X) envelope.getMinX else headPoint.X
          val y_min = if (envelope.getMinY < headPoint.Y) envelope.getMinY else headPoint.Y
          val x_max = if (envelope.getMaxX > headPoint.X) envelope.getMaxX else headPoint.X
          val y_max = if (envelope.getMaxY > headPoint.Y) envelope.getMaxY else headPoint.Y
          get_envelope(tailPoints, new Envelope(x_min, x_max, y_min, y_max))
        }
      }
    }

    // the main call...
    get_envelope(tree.values.toList, new Envelope())
  }
}
