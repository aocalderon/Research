package edu.ucr.dblab.pflock.welzl

import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Point}
import org.apache.commons.numbers.fraction.BigFraction

object Welzl {
  implicit val generator = DiskGenerator
  implicit val rand = scala.util.Random

  def mtfdisk(points: List[Point])(implicit geofactory: GeometryFactory): EnclosingDisk = {
    val support = List.empty[Point]
    b_mtfdisk(points, support, 0)
  }

  private def b_mtfdisk(P: List[Point], R: List[Point], i: Int)
    (implicit geofactory: GeometryFactory, r: scala.util.Random): EnclosingDisk = {
    var minimumCircle: EnclosingDisk = null

    //println("P")
    //P.foreach(println)

    if (R.size == 3) {
      //println("3")
      minimumCircle = generator.ballOnSupport(R)
    }
    else if (P.isEmpty && R.size == 2) {
      //println("2a")
      minimumCircle = generator.ballOnSupport(R)
    }
    else if (P.size == 1 && R.size == 1) {
      //println("2b")
      minimumCircle = generator.ballOnSupport(P ++ R)
    }
    else if (P.size == 1 && R.isEmpty) {
      //println("1")
      minimumCircle = generator.ballOnSupport(P)
    }
    else {
      val i = r.nextInt(P.size)
      val p = P(i)
      minimumCircle = b_mtfdisk(P.filter(_ != p), R, i + 1)

      println("R  : " + R)
      println("mce: " + minimumCircle)
      println("p  : " + p)
      //println("pCm: " + !minimumCircle.contains(p, 0.001))
      if (minimumCircle != null && !minimumCircle.contains(p, 0.001)) {
        //println("mce does not contains p")
	minimumCircle = b_mtfdisk(P.filter(_ != p), R :+ p, i + 10)
      }
    }
    
    minimumCircle
  }

  def main(args: Array[String]): Unit = {
    implicit val geofactory = new GeometryFactory
    val rand = scala.util.Random

    def getRandomPoints(n: Int): List[Point] = {
      {0 until rand.nextInt(n) + 1}.map{ i =>
        val x = rand.nextDouble * 10
        val y = rand.nextDouble * 10
        val coord = new Coordinate(x, y)
        geofactory.createPoint(coord)
      }.toList
    }

    val points = getRandomPoints(30)
    points.foreach(println)
    val disk = Welzl.mtfdisk(points)
    println(disk)

    /*
    (0 until 10).map{ i =>
      val points = getRandomPoints(30)
      val disk = Welzl.mtfdisk(points)
      (disk, points.size)
    }.foreach(println)
     */

  }
}
