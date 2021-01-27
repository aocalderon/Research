package edu.ucr.dblab.pflock.welzl

import com.vividsolutionsOA.jts.geom.Point

case class EnclosingDisk(center: Point, radius: Double, support: Set[Point]) {

  def contains(point: Point, tolerance: Double = 1e-14): Boolean = {
    point.distance(center) <= radius + tolerance
  }

  override def toString = s"[Center: $center Radius: $radius]\n" +
  support.map(s => s"${s.toText} ${s.getUserData}").mkString("\n") + "\n"
}
