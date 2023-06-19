package edu.ucr.dblab.sitester

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.index.strtree._
//import org.locationtech.jts.index.hprtree._
//import org.locationtech.jts.index.quadtree._
import org.locationtech.index.kdtree._

import edu.ucr.dblab.pflock.sedona.{StandardQuadTree, QuadRectangle}

import org.apache.sedona.core.spatialPartitioning.KDB

import edu.ucr.dblab.sitester.spmf.{KDNode, KDTree => SPMFKDTree}
import edu.ucr.dblab.sitester.spmf.DoubleArray

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.Source

import SITester.readPoints

import org.apache.sedona.core.spatialPartitioning.RtreePartitioning

object MBRExtractor {
  case class SedonaEnvelope(envelope: Envelope, point: Point) extends Envelope(envelope)

  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new Params(args)
    implicit val geofactory = new GeometryFactory(new PrecisionModel(
      1.0 / params.tolerance()
    ))

    val points = readPoints(params.input())
    val rtree = new STRtree()
    points.foreach{ point =>
      rtree.insert(point.getEnvelopeInternal, point)
    }

    val kdtree = new KdTree(200.0)
    points.foreach{ point =>
      kdtree.insert(point.getCoordinate, point)
    }

    val g = new java.io.PrintWriter("/tmp/edgesKdNodes.wkt")
    g.write{
      kdtree.query2().asScala.map{ node_prime =>
        val node = node_prime.asInstanceOf[KdNode]
        val data = node.getData.toString()
        s"${geofactory.createPoint(new Coordinate(node.getX, node.getY)).toText}\t${data}\n"
      }.mkString("")
    }
    g.close

    val minX = points.map(_.getX).min
    val minY = points.map(_.getY).min
    val maxX = points.map(_.getX).max
    val maxY = points.map(_.getY).max
    val envelope = new Envelope(minX, maxX, minY, maxY)
    val sedona_kdtree = new KDB(200, 16, envelope)
    points.foreach{ point =>
      val envelope = SedonaEnvelope(point.getEnvelopeInternal, point)
      sedona_kdtree.insert(envelope)
    }
    val hood = sedona_kdtree.findLeafNodes(envelope).asScala.toList
      .map{ node => geofactory.toGeometry(node.getExtent).toText + "\n" }.mkString("")
    val f = new java.io.PrintWriter("/tmp/edgesKDB.wkt")
    f.write(hood)
    f.close

    val partitions = new RtreePartitioning(rtree)
    val h = new java.io.PrintWriter("/tmp/edgesCells.wkt")
    h.write{
      partitions.getGrids.asScala.map{ e =>
        
        geofactory.toGeometry(e).toText + "\n"}.mkString("")
    }
    h.close
  }
}
