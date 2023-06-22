package edu.ucr.dblab.pflock.test

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Coordinate, Point}
import scala.io.Source
import scala.collection.mutable.ListBuffer

import edu.ucr.dblab.pflock.welzl.Welzl
import edu.ucr.dblab.pflock.Utils.{Settings, save}

object Checker {
  def main(args: Array[String]): Unit = {
    implicit val geofactory = new GeometryFactory(new PrecisionModel(1e3))
    implicit val settings = Settings(tolerance = 1e-3)

    val pointsFile = args(1)
    val bufferPoints = Source.fromFile(pointsFile)
    val points = bufferPoints.getLines.map{ line =>
      val arr = line.split("\t")
      val id = arr(0).toInt
      val x = arr(1).toDouble
      val y = arr(2).toDouble
      val p = geofactory.createPoint(new Coordinate(x, y))
      p.setUserData(id)
      (id -> p)
    }.toMap
    bufferPoints.close

    val bufferMaximals = Source.fromFile(args(0))
    val maximals = bufferMaximals.getLines.map{ line =>
      val pids = line.substring(2, line.size).split(" ").map(_.toInt).toList.sorted
      val pts_prime = pids.map{ pid =>
        val p = points(pid)
        p
      }
      val pts = new ListBuffer[Point]()
      pts.appendAll(pts_prime)
      val mbc = Welzl.mbc(pts.toList)

      println(line)
      println(mbc.getRadius)
      
      (mbc, line, pts)
    }.toList
    bufferMaximals.close
  }
}
