package edu.ucr.dblab.pflock.welzl

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point, PrecisionModel}
import edu.ucr.dblab.pflock.welzl.sec.SmallestEnclosingCircle
import scala.collection.mutable.ListBuffer
import collection.JavaConverters._
import scala.util.Random

object Welzl {
  implicit val generator: DiskGenerator.type = DiskGenerator
  implicit val rand: Random.type = scala.util.Random

  def mtfdisk(P: ListBuffer[Point])(implicit geofactory: GeometryFactory): EnclosingDisk = {
    val R = Set[Point]()
    b_mtfdisk(P, R)
  }

  private def b_mtfdisk(P: ListBuffer[Point], R: Set[Point])
    (implicit geofactory: GeometryFactory,
      rand: scala.util.Random): EnclosingDisk = {
    if (P.isEmpty || R.size == 3) {
      generator.ballOnSupport(R)
    } else if (P.size <= 2 && R.isEmpty) {
      generator.ballOnSupport(P.toSet)
    } else {
      val p = P.remove(P.size - 1)

      //println("P: " + P)
      //println("R: " + R)
      //println("p: " + p)

      val disk = b_mtfdisk(P, R)

      //println("Disk: " + disk)

      if (!disk.contains(p)) {
        val new_R = R + p
        val disk_prime = b_mtfdisk(P, new_R)
        //println("MTF: " + P)
        P.insert(0, p)
        //println("MTF: " + p + " -> " + P)
        disk_prime
      } else {
        disk
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1e3)
    implicit val geofactory: GeometryFactory = new GeometryFactory(model)

    def getRandomPoints(n: Int): Seq[Point] = {
      if (rand.nextDouble() < 0.2) {
        (0 to n) map { _ =>
          val x = rand.nextInt(10)
          val y = rand.nextInt(10)
          val coord = new Coordinate(x, y)
          geofactory.createPoint(coord)
        }
      } else {
        (0 to n) map { _ =>
          val x = rand.nextGaussian()
          val y = rand.nextGaussian()
          val coord = new Coordinate(x, y)
          geofactory.createPoint(coord)
        }
      }
    }
    import scala.io.Source
    val bufferPoints = Source.fromFile("/home/and/Research/Datasets/Test/welzl.txt")
    val points_prime = bufferPoints.getLines.map{ line =>
      val arr = line.split("\t")
      val id = arr(0).toInt
      val x = arr(1).toDouble
      val y = arr(2).toDouble
      val p = geofactory.createPoint(new Coordinate(x, y))
      p.setUserData(id)
      p
    }.toList
    bufferPoints.close
    val points = new ListBuffer[Point]()
    points.appendAll(points_prime)
    for( i <- 0 to 10) {
      val disk = Welzl.mtfdisk(points)
      println(disk)
    }

    /*
    val t0 = System.currentTimeMillis()
    val r = (0 until 25000).map{ _ =>
      val pList = getRandomPoints(rand.nextInt(30) + 1)
      val points = new ListBuffer[Point]()
      points.appendAll(pList)
      val actual = Welzl2.mtfdisk(points)
      val points2 = points.map{ p=>
        val c = p.getCoordinate
        new edu.ucr.dblab.pflock.welzl.sec.Point(c.x, c.y)
      }.asJava
      val reference = SmallestEnclosingCircle.naive(points2)
      val epsilon = 1e-14
      if(math.abs(reference.r - actual.radius) < epsilon &&
            math.abs(reference.c.x - actual.center.getX) < epsilon &&
            math.abs(reference.c.y - actual.center.getY) < epsilon){
        1
      } else {
        0
      }
    }
    val t1 = (System.currentTimeMillis() - t0) / 1e3
    println(s"${r.sum} tests passed in $t1 sec.")*/

  }
}
