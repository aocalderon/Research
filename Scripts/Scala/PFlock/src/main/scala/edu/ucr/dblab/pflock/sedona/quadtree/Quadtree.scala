package edu.ucr.dblab.pflock.sedona.quadtree

import org.locationtech.jts.geom.{GeometryFactory, Envelope, Coordinate, Polygon, Point}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.io.WKTReader
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import java.io.FileWriter
import scala.io.Source

import edu.ucr.dblab.pflock.Utils.Cell

object Quadtree {  
  def create[T](boundary: Envelope, lineages: List[String]): StandardQuadTree[T] = {
    val quadtree = if(lineages.size < 4){  // If quadtree has only one level,
                                           // we just return a simplre quadtree with the boundary...
      new StandardQuadTree[T](new QuadRectangle(boundary), 0, 1, 0)
    } else {
      val maxLevel = lineages.map(_.size).max
      val quadtree = new StandardQuadTree[T](new QuadRectangle(boundary), 0, 1, maxLevel)
      quadtree.split()
      for(lineage <- lineages.sorted){
        val arr = lineage.map(_.toInt - 48)
        var current = quadtree
        for(position <- arr.slice(0, arr.size - 1)){
          val regions = current.getRegions()
          current = regions(position)
          if(current.getRegions == null){
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
    for(position <- lineage.map(_.toInt - 48)){
      val regions = current.getRegions()
      current = regions(position)
    }

    current.assignPartitionLineage()
    current.assignPartitionIds()

    current
  }

  def save[T](quadtree: StandardQuadTree[T], filename: String): Unit = {
    val f = new FileWriter(filename)
    val WKT = quadtree.getLeafZones.asScala.map{ leaf =>
      val wkt = leaf.wkt
      val cid = leaf.partitionId
      val lin = leaf.lineage

      s"$wkt\t$cid\t$lin\n"
    }.mkString("")
    f.write(WKT)
    f.close
  }

  def load[T](filename: String)(implicit geofactory: GeometryFactory): StandardQuadTree[T] = {
    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(filename)
    val cells = buffer.getLines.toList.map{ line =>
      val arr = line.split("\t")
      val envelope = reader.read(arr(0)).asInstanceOf[Polygon].getEnvelopeInternal
      val lineage = arr(1)
      (lineage, envelope)
    }
    buffer.close()
    val left  = cells.map(_._2.getMinX).min
    val right = cells.map(_._2.getMaxX).max
    val down  = cells.map(_._2.getMinY).min
    val up    = cells.map(_._2.getMaxY).max
    val boundary = new Envelope(left, right, down, up)
    val lineages = cells.map(_._1)

    create[T](boundary, lineages)
  }

  def extract[T](quadtree: StandardQuadTree[T], lineage: String): StandardQuadTree[T] = {
    var current = quadtree
    for(position <- lineage.map(_.toInt - 48)){
      val regions = current.getRegions()
      current = regions(position)
    }

    current
  }

  def filter[T](quadtree: StandardQuadTree[T], filter: String): StandardQuadTree[T] = {
    if(filter == "*"){
      quadtree
    } else {
      var current = quadtree
      for(position <- filter.map(_.toInt - 48)){
        val regions = current.getRegions()
        current = regions(position)
      }

      current
    }
  }

  def build(points: RDD[Point], envelope: Envelope = new Envelope(), capacity: Int = 100,
    fraction: Double = 0.05, maxLevel: Int = 16, level: Int = 0, seed: Int = 42)
      : (Map[Int, Envelope], STRtree, Envelope) = {

    val rectangle = if(envelope.isNull){
      val Xs   = points.map(_.getX).cache
      val Ys   = points.map(_.getY).cache
      val minX = Xs.min()
      val maxX = Xs.max()
      val minY = Ys.min()
      val maxY = Ys.max()
      val enve = new Envelope(minX, maxX, minY, maxY)
      new QuadRectangle(enve)
    } else {
      new QuadRectangle(envelope)
    }

    val G = new GeometryFactory()
    //println(G.toGeometry(rectangle.getEnvelope).toText)

    val quadtree = new StandardQuadTree[Point](rectangle, level, capacity, maxLevel)

    points.sample(false, fraction, seed)
      .collect
      .foreach{ point =>
        val mbr = new QuadRectangle(point.getEnvelopeInternal)
        quadtree.insert(mbr, point)
      }
    quadtree.assignPartitionIds
    quadtree.assignPartitionLineage
    quadtree.dropElements

    val tree  = new STRtree()
    val cells = quadtree.getLeafZones.asScala.map{ leaf =>
      val  id = leaf.partitionId.toInt
      val env = leaf.getEnvelope

      tree.insert(env, id)

      (id -> env)
    }.toMap

    (cells, tree, rectangle.getEnvelope)
  }

  def read(cells_file: String, boundary_file: String)(implicit geofactory: GeometryFactory)
      : (Map[Int, Envelope], STRtree, Envelope) = {

    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(cells_file)
    val tree   = new STRtree()
    val cells = buffer.getLines.toList.map{ line =>
      val arr = line.split("\t")
      val envelope = reader.read(arr(0)).asInstanceOf[Polygon].getEnvelopeInternal
      val cellId   = arr(1).toInt

      tree.insert(envelope, cellId)
      (cellId, envelope)
    }.toMap
    buffer.close()

    val buffer2 = Source.fromFile(boundary_file)
    val boundary = reader.read(buffer2.getLines.next.split("\t")(0))
      .asInstanceOf[Polygon].getEnvelopeInternal
    buffer2.close

    (cells, tree, boundary)
  }

  /* Quite specific to PFlock project */
  import edu.ucr.dblab.pflock.Utils.{Settings, Cell}
  def loadCells(filename: String, wkt_position: Int = 0, id_position: Int = 1)
    (implicit G: GeometryFactory): Map[Int, Cell] = {

    val reader = new WKTReader(G)
    val buffer = Source.fromFile(filename)
    val cells = buffer.getLines.map{ line =>
      val arr = line.split("\t")
      val envelope = reader.read(arr(wkt_position)).asInstanceOf[Polygon].getEnvelopeInternal
      val id = arr(id_position).toInt
      (id, Cell(mbr = envelope, cid = id, lineage = ""))
    }.toMap
    buffer.close()

    cells
  }

  def getQuadtreeFromPoints(points: RDD[Point], expand: Boolean = true, level: Int = 0,
    maxLevel: Int = 16) (implicit S: Settings): StandardQuadTree[Point] = {

    val sample = points.sample(false, S.fraction, 42).collect().toList
    val minx = points.map(_.getX).min
    val miny = points.map(_.getY).min
    val maxx = points.map(_.getX).max
    val maxy = points.map(_.getY).max
    val envelope = new Envelope(new Coordinate(minx, miny), new Coordinate(maxx, maxy))
    if(expand) envelope.expandBy(S.epsilon) // For PFlock project,
                                            // add a pad around the study area for possible disks ...
    val rectangle = new QuadRectangle(envelope)
    val quadtree = new StandardQuadTree[Point](rectangle, level, S.capacity, maxLevel)

    sample.foreach { point =>
      val mbr = new QuadRectangle(point.getEnvelopeInternal)
      quadtree.insert(mbr, point)
    }
    quadtree.assignPartitionIds
    quadtree.assignPartitionLineage

    quadtree.dropElements
    quadtree
  }

  def getQuadtreeFromPointList[T](points: List[Point], expand: Boolean = true, level: Int = 0,
    maxLevel: Int = 16) (implicit S: Settings): StandardQuadTree[Point] = {

    val minx = points.map(_.getX).min
    val miny = points.map(_.getY).min
    val maxx = points.map(_.getX).max
    val maxy = points.map(_.getY).max
    val envelope = new Envelope(new Coordinate(minx, miny), new Coordinate(maxx, maxy))
    if(expand) envelope.expandBy(S.epsilon) // For PFlock project,
                                            // add a pad around the study area for possible disks ...
    val rectangle = new QuadRectangle(envelope)
    val quadtree = new StandardQuadTree[Point](rectangle, level, S.capacity, maxLevel)

    points.foreach { point =>
      val mbr = new QuadRectangle(point.getEnvelopeInternal)
      quadtree.insert(mbr, point)
    }
    quadtree.assignPartitionIds
    quadtree.assignPartitionLineage
    
    quadtree
  }

  def loadQuadtreeAndCells[T](filename: String)
    (implicit S: Settings, geofactory: GeometryFactory): (StandardQuadTree[T], Map[Int, Cell]) = {
    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(filename)
    val cells_prime = buffer.getLines.toList.map{ line =>
      val arr = line.split("\t")
      val mbr = reader.read(arr(0)).asInstanceOf[Polygon].getEnvelopeInternal
      val lin = arr(1)
      val cid = arr(2).toInt
      Cell(mbr, cid, lin)
    }
    buffer.close()

    val left  = cells_prime.map(_.mbr.getMinX).min
    val right = cells_prime.map(_.mbr.getMaxX).max
    val down  = cells_prime.map(_.mbr.getMinY).min
    val up    = cells_prime.map(_.mbr.getMaxY).max
    val boundary = new Envelope(left, right, down, up)
    boundary.expandBy(S.epsilon)

    val lineages = cells_prime.map(_.lineage)

    val quadtree = create[T](boundary, lineages)
    val cells = cells_prime.map(cell => cell.cid -> cell).toMap

    (quadtree, cells)
  }
}
