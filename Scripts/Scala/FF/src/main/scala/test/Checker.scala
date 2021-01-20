package edu.ucr.dblab.pflock.test

import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel, Coordinate, Point}
import scala.io.Source
import scala.collection.mutable.ListBuffer

import edu.ucr.dblab.pflock.welzl.Welzl
import edu.ucr.dblab.pflock.Utils.save

object Checker {
  def main(args: Array[String]): Unit = {
    implicit val geofactory = new GeometryFactory(new PrecisionModel(1e3))

    val bufferPoints = Source.fromFile("/home/and/Research/Datasets/dense2.tsv")
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
      val mbc = Welzl.mtfdisk(pts)

      println(line)
      println(mbc.radius)
      //println
      
      (mbc, line, pts)
    }.toList
    bufferMaximals.close

    //maximals foreach println
    save("/tmp/edgesSample.wkt"){
      val l = List(269487, 350899, 567374, 697234, 823311, 1029111, 2045212, 2493948, 2562635, 3183122, 3736569, 3830016, 3851136, 4130738)
      l.map(p => points(p)).map{ p =>
        val i = p.getUserData
        val x = p.getX
        val y = p.getY
        s"$i\t$x\t$y\n"
      }
    }
    save("/tmp/edgesCheck.wkt"){
      maximals.map{ case(mbc, line, pts) =>
        val r = mbc.radius
        val wkt = mbc.center.buffer(r, 15).toText
        s"$wkt\t$r\t$line\n"
      }
    }
  }
}
