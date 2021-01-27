package edu.ucr.dblab.pflock.welzl

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}
import org.apache.commons.numbers.fraction.BigFraction

object DiskGenerator {

  def ballOnSupport(support: Set[Point])
    (implicit geofactory: GeometryFactory): EnclosingDisk = {

    support.size match {
      case 0 => {
        val emptyPoint =
          geofactory.createPoint(new Coordinate(Double.MinValue, Double.MinValue))
        EnclosingDisk(emptyPoint, Double.MinValue, support)
      }
      case 1 => EnclosingDisk(support.head, 0, support)
      case 2 => {
        val S = support.toList
        val A = S(0)
        val B = S(1)
        val center = {
          val x = if (A.getX < B.getX) {
            A.getX + ((B.getX - A.getX) * 0.5)
          } else {
            B.getX + ((A.getX - B.getX) * 0.5)
          }
          val y = if (A.getY < B.getY) {
            A.getY + ((B.getY - A.getY) * 0.5)
          } else {
            B.getY + ((A.getY - B.getY) * 0.5)
          }
          geofactory.createPoint(new Coordinate(x, y))
        }
        val radius = A.distance(B) * 0.5

        EnclosingDisk(center, radius, support)
      }
      case 3 => {
        val S = support.toList
        val vA = S(0)
        val vB = S(1)
        val vC = S(2)
        val c2 = Array(
          BigFraction.from(vA.getX()),
          BigFraction.from(vB.getX()),
          BigFraction.from(vC.getX()))
        val c3 = Array(
          BigFraction.from(vA.getY()),
          BigFraction.from(vB.getY()),
          BigFraction.from(vC.getY()))
        val c1 = Array(
          c2(0).multiply(c2(0)).add(c3(0).multiply(c3(0))),
          c2(1).multiply(c2(1)).add(c3(1).multiply(c3(1))),
          c2(2).multiply(c2(2)).add(c3(2).multiply(c3(2))))
        val twoM11 = minor(c2, c3).multiply(2)

        if (twoM11.doubleValue() != 0.0) {
          val m12 = minor(c1, c3)
          val m13 = minor(c1, c2)
          val centerX = m12.divide(twoM11)
          val centerY = m13.divide(twoM11).negate
          val dx = c2(0).subtract(centerX)
          val dy = c3(0).subtract(centerY)
          val r2 = dx.multiply(dx).add(dy.multiply(dy))
          val coord = new Coordinate(centerX.doubleValue(), centerY.doubleValue())
          val center = geofactory.createPoint(coord)
          val radius = math.sqrt(r2.doubleValue())

          EnclosingDisk(center, radius, support)
        } else {
          val ab = vA.distance(vB)
          val ac = vA.distance(vC)
          val bc = vB.distance(vC)
          if (ab > ac && ab > bc) ballOnSupport(Set(vA, vB))
          else if (ac > ab && ac > bc) ballOnSupport(Set(vA, vC))
          else ballOnSupport(Set(vB, vC))
        }
      }
    }
  }

  private def minor(c1: Array[BigFraction], c2: Array[BigFraction]): BigFraction = {
    c2(0).multiply(c1(2).subtract(c1(1)))
      .add(c2(1).multiply(c1(0).subtract(c1(2))))
      .add(c2(2).multiply(c1(1).subtract(c1(0))))
  }
}
