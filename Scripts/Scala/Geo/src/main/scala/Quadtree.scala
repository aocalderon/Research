package edu.ucr.dblab

import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import org.datasyslab.geospark.spatialPartitioning.quadtree._
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Polygon}
import com.vividsolutions.jts.geom.GeometryFactory
import scala.annotation.tailrec
import edu.ucr.dblab.Utils._

class Quadtree(zone: QuadRectangle, level: Int, maxItemsPerZone: Int, maxLevel: Int, lineage: String)
    extends StandardQuadTree(zone, level, maxItemsPerZone, maxLevel) {

  var regions = List.empty[Quadtree]

  def newQuadtree(zone: QuadRectangle, level: Int, lineage: String): Quadtree = {
    new Quadtree(zone, level, this.maxItemsPerZone, this.maxLevel, lineage);
  }

  def split(): List[Quadtree] = {
    val newWidth = zone.width / 2;
    val newHeight = zone.height / 2;
    val newLevel = level + 1;

    val NW = newQuadtree(new QuadRectangle(
      zone.x,
      zone.y + zone.height / 2,
      newWidth,
      newHeight
    ), newLevel, lineage + "0")

    val NE = newQuadtree(new QuadRectangle(
      zone.x + zone.width / 2,
      zone.y + zone.height / 2,
      newWidth,
      newHeight
    ), newLevel, lineage + "1")

    val SW = newQuadtree(new QuadRectangle(
      zone.x,
      zone.y,
      newWidth,
      newHeight
    ), newLevel, lineage + "2")

    val SE = newQuadtree(new QuadRectangle(
      zone.x + zone.width / 2,
      zone.y,
      newWidth,
      newHeight
    ), newLevel, lineage + "3")

    List(NW, NE, SW, SE)
  }

  def setRegions(): Unit = {
    this.regions = this.split()
  }

  override def toString(): String = lineage

}

object Quadtree {
  @tailrec
  def walk(path: List[Int], node: Quadtree): Quadtree = {
    path match {
      case Nil => node
      case x :: tail => {
        if(node.regions.isEmpty) node.setRegions()
        println(s"x: $x tail: ${tail.mkString(" ")} node: ${node.toString()}")
        walk(tail, node.regions(x))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val envelope = new Envelope(new Coordinate(0,0), new Coordinate(0,0))
    val root = new Quadtree(new QuadRectangle(envelope), 0, 1, 3, "")
    val path = List(0,1,3)

    Quadtree.walk(path, root)

    root.assignPartitionIds()
    root.assignPartitionLineage()

    root.regions.foreach { println }
  }
}
