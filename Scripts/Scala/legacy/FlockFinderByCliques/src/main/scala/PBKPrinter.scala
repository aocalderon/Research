package edu.ucr.dblab

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Polygon, Point, Coordinate, MultiPoint}
import scala.io.Source

object PBKPrinter {
  case class SPoint(point: Point, id: Int)
  case class Clique(hull: Polygon, points: MultiPoint)
  def main(args: Array[String]): Unit = {
    val geofactory = new GeometryFactory(new PrecisionModel(1000))

    val bufferPoints = Source.fromFile(args(0))
    val points = bufferPoints.getLines.map{ line =>
      val arr = line.split("\t")
      val id = arr(0).toInt
      val x  = arr(1).toDouble
      val y  = arr(2).toDouble
      val point = geofactory.createPoint(new Coordinate(x, y))
      (id, point)
    }.toList.distinct.toMap
    bufferPoints.close

    val bufferCliques = Source.fromFile(args(1))
    val cliques = bufferCliques.getLines.map{ line =>
      val ids = line.split(" ").map(_.toInt)
      val pts = ids.map(id => points(id))
      val mpts = geofactory.createMultiPoint(pts.map(_.getCoordinate))
      val hull = mpts.convexHull.asInstanceOf[Polygon]
      Clique(hull, mpts)
    }.toList
    bufferCliques.close

    save("/tmp/edgesCliquesHull.wkt"){
      cliques.map{ clique =>
        val wkt = clique.hull.toText
        s"$wkt\n"
      }
    }
    save("/tmp/edgesCliquesPoints.wkt"){
      cliques.map{ clique =>
        val wkt = clique.points.toText
        s"$wkt\n"
      }
    }
  }

  import java.io.PrintWriter
  def save(filename: String)(content: Seq[String]): Unit = {
    val f = new PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    println(s"Saved $filename [${content.size} records].")
  }
  
}
