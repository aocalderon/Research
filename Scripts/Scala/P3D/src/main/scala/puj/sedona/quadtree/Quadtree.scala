package edu.ucr.dblab.pflock.sedona.quadtree

import org.locationtech.jts.geom.{GeometryFactory, Envelope, Coordinate, Polygon, Point}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.io.WKTReader
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import java.io.FileWriter
import scala.io.Source

object Quadtree {

  case class Cell(id: Int, envelope: Envelope, lineage: String = "")(implicit G: GeometryFactory) {
    def wkt: String = G.toGeometry(envelope).toText

    override def toString(): String = s"$wkt\t$id\t$lineage"

    def toText: String = s"${toString()}\n"
  }

  def create[T](boundary: Envelope, lineages: List[String]): StandardQuadTree[T] = {
    val quadtree = if (lineages.size < 4) { // If quadtree has only one level,
      // we just return a simplre quadtree with the boundary...
      new StandardQuadTree[T](new QuadRectangle(boundary), 0, 1, 0)
    } else {
      val maxLevel = lineages.map(_.size).max
      val quadtree = new StandardQuadTree[T](new QuadRectangle(boundary), 0, 1, maxLevel)
      quadtree.split()
      for (lineage <- lineages.sorted) {
        val arr     = lineage.map(_.toInt - 48)
        var current = quadtree
        for (position <- arr.slice(0, arr.size - 1)) {
          val regions = current.getRegions()
          current = regions(position)
          if (current.getRegions == null) {
            current.split()
          }
        }
      }
      quadtree
    }
    quadtree.assignPartitionLineage()
    quadtree.assignPartitionIds()

    quadtree
  }

  def get[T](quadtree: StandardQuadTree[T], lineage: String): StandardQuadTree[T] = {
    var current = quadtree
    for (position <- lineage.map(_.toInt - 48)) {
      val regions = current.getRegions()
      current = regions(position)
    }

    current.assignPartitionLineage()
    current.assignPartitionIds()

    current
  }

  def save[T](quadtree: StandardQuadTree[T], filename: String): Unit = {
    val f   = new FileWriter(filename)
    val WKT = quadtree.getLeafZones.asScala
      .map { leaf =>
        val wkt = leaf.wkt
        val cid = leaf.partitionId
        val lin = leaf.lineage

        s"$wkt\t$cid\t$lin\n"
      }
      .mkString("")
    f.write(WKT)
    f.close
  }

  def load[T](filename: String)(implicit geofactory: GeometryFactory): StandardQuadTree[T] = {
    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(filename)
    val cells  = buffer.getLines.toList.map { line =>
      val arr      = line.split("\t")
      val envelope = reader.read(arr(0)).asInstanceOf[Polygon].getEnvelopeInternal
      val lineage  = arr(1)
      (lineage, envelope)
    }
    buffer.close()
    val left     = cells.map(_._2.getMinX).min
    val right    = cells.map(_._2.getMaxX).max
    val down     = cells.map(_._2.getMinY).min
    val up       = cells.map(_._2.getMaxY).max
    val boundary = new Envelope(left, right, down, up)
    val lineages = cells.map(_._1)

    create[T](boundary, lineages)
  }

  def extract[T](quadtree: StandardQuadTree[T], lineage: String): StandardQuadTree[T] = {
    var current = quadtree
    for (position <- lineage.map(_.toInt - 48)) {
      val regions = current.getRegions()
      current = regions(position)
    }

    current
  }

  def filter[T](quadtree: StandardQuadTree[T], filter: String): StandardQuadTree[T] = {
    if (filter == "*") {
      quadtree
    } else {
      var current = quadtree
      for (position <- filter.map(_.toInt - 48)) {
        val regions = current.getRegions()
        current = regions(position)
      }

      current
    }
  }

  def build(
      points: RDD[Point],
      envelope: Envelope = new Envelope(),
      capacity: Int = 100,
      fraction: Double = 0.05,
      maxLevel: Int = 16,
      level: Int = 0,
      seed: Int = 42
  )(implicit G: GeometryFactory): (StandardQuadTree[Point], Map[Int, Cell], STRtree, Envelope) = {

    val universe = if (envelope.isNull) {
      val Xs             = points.map(_.getX).cache
      val Ys             = points.map(_.getY).cache
      val minX           = Xs.min() - 1.0
      val maxX           = Xs.max() + 1.0
      val minY           = Ys.min() - 1.0
      val maxY           = Ys.max() + 1.0
      val envelope_prime = new Envelope(minX, maxX, minY, maxY)
      new QuadRectangle(envelope_prime)
    } else {
      new QuadRectangle(envelope)
    }

    val quadtree = new StandardQuadTree[Point](universe, level, capacity, maxLevel)

    points
      .sample(false, fraction, seed)
      .collect
      .foreach { point =>
        val mbr = new QuadRectangle(point.getEnvelopeInternal)
        quadtree.insert(mbr, point)
      }
    quadtree.assignPartitionIds
    quadtree.assignPartitionLineage
    quadtree.dropElements

    val rtree = new STRtree()
    val cells = quadtree.getLeafZones.asScala.map { leaf =>
      val id       = leaf.partitionId.toInt
      val envelope = leaf.getEnvelope
      val linage   = leaf.lineage

      val cell = Cell(id, envelope, linage)

      rtree.insert(envelope, id)
      (id -> cell)
    }.toMap

    (quadtree, cells, rtree, universe.getEnvelope)
  }

  def read(cells_file: String, boundary_file: String)(implicit geofactory: GeometryFactory): (Map[Int, Envelope], STRtree, Envelope) = {

    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(cells_file)
    val tree   = new STRtree()
    val cells  = buffer.getLines.toList.map { line =>
      val arr      = line.split("\t")
      val envelope = reader.read(arr(0)).asInstanceOf[Polygon].getEnvelopeInternal
      val cellId   = arr(1).toInt

      tree.insert(envelope, cellId)
      (cellId, envelope)
    }.toMap
    buffer.close()

    val buffer2  = Source.fromFile(boundary_file)
    val boundary = reader
      .read(buffer2.getLines.next.split("\t")(0))
      .asInstanceOf[Polygon]
      .getEnvelopeInternal
    buffer2.close

    (cells, tree, boundary)
  }
}
