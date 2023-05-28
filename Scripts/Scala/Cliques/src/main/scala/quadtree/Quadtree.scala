package edu.ucr.dblab.pflock.quadtree

import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point}
import scala.collection.JavaConverters._
import java.io.FileWriter

object Quadtree {  
  def create[T](boundary: Envelope, lineages: List[String]): StandardQuadTree[T] = {
    val quadtree = if(lineages.size < 4){  // If quadtree has only one level, we just return a simplre quadtree with the boundary...
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

  def getQuadtreeFromPoints(points: List[Point], capacity: Int, level: Int = 0, maxLevel: Int = 16): StandardQuadTree[Point] = {
    val minx = points.minBy(_.getX).getX
    val miny = points.minBy(_.getY).getY
    val maxx = points.maxBy(_.getX).getX
    val maxy = points.maxBy(_.getY).getY
    val envelope = new Envelope(new Coordinate(minx, miny), new Coordinate(maxx, maxy))
    val rectangle = new QuadRectangle(envelope)
    val quadtree = new StandardQuadTree[Point](rectangle, level, capacity, maxLevel)

    points.foreach { point =>
      val mbr = new QuadRectangle(point.getEnvelopeInternal)
      quadtree.insert(mbr, point)
    }
    quadtree.assignPartitionIds
    quadtree.assignPartitionLineage
    
    quadtree
  }  
}