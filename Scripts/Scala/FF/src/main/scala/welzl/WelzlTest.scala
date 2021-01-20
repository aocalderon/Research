package edu.ucr.dblab.pflock.welzl

import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, PrecisionModel}
import org.apache.commons.geometry.enclosing.euclidean.twod.WelzlEncloser2D
import org.apache.commons.geometry.enclosing.EnclosingBall
import org.apache.commons.geometry.core.precision.DoublePrecisionContext
import org.apache.commons.geometry.core.precision.EpsilonDoublePrecisionContext
import org.apache.commons.geometry.euclidean.twod.Vector2D

import collection.JavaConverters._
import scala.io.Source

object Welzl2 {
  def encloser(implicit geofactory: GeometryFactory): WelzlEncloser2D = {
    val TEST_EPS = 1.0 / geofactory.getPrecisionModel.getScale 
    val TEST_PRECISION =  new EpsilonDoublePrecisionContext(TEST_EPS)

    new WelzlEncloser2D(TEST_PRECISION: DoublePrecisionContext)
  }

  def mbc(points: List[Point])
    (implicit geofactory: GeometryFactory): EnclosingBall[Vector2D] = {

    encloser.enclose(asVectors(points))
  }

  def asVectors(points: List[Point]) = points.map(point2vector).toIterable.asJava

  private def point2vector(point: Point): Vector2D = Vector2D.of(point.getX, point.getY)

  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1e3)
    implicit val geofactory: GeometryFactory = new GeometryFactory(model)

    val bufferPoints = Source.fromFile("/home/and/Research/Datasets/Test/welzl.txt")
    val points = bufferPoints.getLines.map{ line =>
      val arr = line.split("\t")
      val id = arr(0).toInt
      val x = arr(1).toDouble
      val y = arr(2).toDouble
      val p = geofactory.createPoint(new Coordinate(x, y))
      p.setUserData(id)
      p
    }.toList
    bufferPoints.close

    val welzl = encloser
    val mbc = welzl.enclose(asVectors(points))

    println(mbc)
  }
}
