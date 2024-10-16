package edu.ucr.dblab.pflock.sedona

import org.locationtech.jts.geom.{GeometryFactory, Envelope, Coordinate, Polygon, Point}
import org.locationtech.jts.io.WKTReader
import scala.collection.JavaConverters._
import java.io.FileWriter
import scala.io.Source

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
      val lin = leaf.lineage
      val cid = leaf.partitionId

      s"$wkt\t$lin\t$cid\n"
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

}
