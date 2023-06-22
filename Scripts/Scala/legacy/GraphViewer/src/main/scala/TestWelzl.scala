package edu.ucr.dblab

import org.apache.commons.math3.geometry.euclidean.twod.DiskGenerator
import org.apache.commons.math3.geometry.euclidean.twod.Vector2D
import org.apache.commons.math3.geometry.enclosing.SupportBallGenerator
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Coordinate, Point}
import scala.collection.JavaConverters._
import scala.io.Source

object TestWelzl {
  case class VPoint(p: Point) extends Vector2D(p.getX, p.getY)
  
  def main(args: Array[String]): Unit = {
    val model  = new PrecisionModel(1000)
    val geofactory = new GeometryFactory(model)
    val input = args(0)

    val pointsBuff = Source.fromFile(input)
    val points = pointsBuff.getLines.map{ line =>
      val arr = line.split("\t")
      val id = arr(0).toInt
      val x = arr(1).toDouble
      val y = arr(2).toDouble

      val coord = new Coordinate(x, y)
      val point = geofactory.createPoint(coord)
      point.setUserData(id)
      point
    }

    val diskGenerator = new DiskGenerator
    val welzl = new Welzl(1.0 / model.getScale, diskGenerator)
    val pIt = points.map[Vector2D]{VPoint}.toIterable
    val sec = welzl.enclose(pIt.asJava)
    sec.getSupport.map{_.asInstanceOf[VPoint].p.getUserData}.foreach{println}
    println(s"The SEC support: ${sec.getSupport.map(_.asInstanceOf[VPoint].p).mkString(" ")}")
  }
}
